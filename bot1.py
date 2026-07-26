"""
BOT 1 — Signal Engine v7 (current live strategy)

This file holds ONLY decision / strategy logic for Bot 1.
app.py loads this module lazily when the user selects Bot 1 in the UI.
Other bot files (bot2/bot3/bot4) stay unloaded so they do not use RAM.

Do not put order placement / Flask routes / DB / websocket code here.
"""

import random
import time as _time
from datetime import datetime

BOT_ID = "bot1"
BOT_NAME = "Bot 1 — Signal Engine v7"
BOT_READY = True  # strategy implemented

# Injected by app.py via bind_runtime() — never import app from here.
_RUNTIME = None


def bind_runtime(runtime):
    """Called once when this bot is activated. runtime provides app helpers."""
    global _RUNTIME
    _RUNTIME = runtime


def _rt():
    if _RUNTIME is None:
        raise RuntimeError("bot1: runtime not bound — activate bot via app first")
    return _RUNTIME


def _get_symbol():
    return _rt()["get_symbol"]()


def _make_api_request(method, path, **kwargs):
    return _rt()["make_api_request"](method, path, **kwargs)


def _get_decision_tf_config():
    return _rt()["get_decision_tf_config"]()


def _decision_tf_priority():
    return _rt()["decision_tf_priority"]


# ========== SIGNAL GENERATION — v7 (FULL INDICATOR SUITE) ==========

def _ema(prices, period):
    if len(prices) < period: return None
    k = 2.0 / (period + 1)
    val = sum(prices[:period]) / period
    for p in prices[period:]: val = p * k + val * (1 - k)
    return val

def _sma(prices, period):
    if len(prices) < period: return None
    return sum(prices[-period:]) / period

def _rsi(closes, period=14):
    if len(closes) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    ag = sum(gains) / period; al = sum(losses) / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (period - 1) + max(d, 0)) / period
        al = (al * (period - 1) + max(-d, 0)) / period
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag / al))

def _macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal: return None, None, None
    ef = _ema(closes, fast); es = _ema(closes, slow)
    if ef is None or es is None: return None, None, None
    ml = ef - es
    series = []
    for i in range(slow - 1, len(closes)):
        ef2 = _ema(closes[:i+1], fast); es2 = _ema(closes[:i+1], slow)
        if ef2 and es2: series.append(ef2 - es2)
    if len(series) < signal: return ml, None, None
    sl_ = _ema(series, signal)
    return ml, sl_, (ml - sl_) if sl_ else None

def _bollinger(closes, period=20, std_dev=2.0):
    if len(closes) < period: return None, None, None
    recent = closes[-period:]; mid = sum(recent) / period
    std = (sum((x - mid) ** 2 for x in recent) / period) ** 0.5
    return mid + std_dev * std, mid, mid - std_dev * std

def _atr(candles, period=14):
    if len(candles) < period + 1: return None
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]['high']; l = candles[i]['low']; pc = candles[i-1]['close']
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    if len(trs) < period: return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]: atr = (atr * (period - 1) + tr) / period
    return atr

def _candle_strength(candles, lookback=5):
    if len(candles) < lookback: return 0
    recent = candles[-lookback:]
    bull = sum(1 for c in recent if c['close'] > c['open'])
    bear = sum(1 for c in recent if c['close'] < c['open'])
    last = candles[-1]; rng = last['high'] - last['low']
    body = abs(last['close'] - last['open'])
    bp = (body / rng) if rng > 0 else 0
    if bull >= 3 and bp > 0.5 and last['close'] > last['open']: return 1
    if bear >= 3 and bp > 0.5 and last['close'] < last['open']: return -1
    return 0

def _wma(prices, period):
    if len(prices) < period: return None
    w = list(range(1, period + 1))
    return sum(wi * p for wi, p in zip(w, prices[-period:])) / sum(w)

def _hma(prices, period=9):
    half = max(period // 2, 1); sq = max(int(period ** 0.5), 1)
    if len(prices) < period: return None
    raw = []
    for i in range(max(period, half), len(prices) + 1):
        wh = _wma(prices[:i], half); wf = _wma(prices[:i], period)
        if wh and wf: raw.append(2 * wh - wf)
    return _wma(raw, sq) if len(raw) >= sq else None

def _ultimate_oscillator(candles, p1=7, p2=14, p3=28):
    if len(candles) < p3 + 1: return None
    def _avg(sl):
        bps, trs = [], []
        for i in range(1, len(sl)):
            pc = sl[i-1]['close']; h = sl[i]['high']; l = sl[i]['low']; c = sl[i]['close']
            bps.append(c - min(l, pc)); trs.append(max(h, pc) - min(l, pc))
        return sum(bps)/sum(trs) if sum(trs) else 0
    return 100 * (4*_avg(candles[-(p1+1):]) + 2*_avg(candles[-(p2+1):]) + _avg(candles[-(p3+1):])) / 7

def _adx(candles, period=14):
    if len(candles) < period * 2 + 1: return None, None, None
    trs, pdms, mdms = [], [], []
    for i in range(1, len(candles)):
        h=candles[i]['high']; l=candles[i]['low']; ph=candles[i-1]['high']
        pl=candles[i-1]['low']; pc=candles[i-1]['close']
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
        pdms.append(max(h-ph, 0) if (h-ph) > (pl-l) else 0)
        mdms.append(max(pl-l, 0) if (pl-l) > (h-ph) else 0)
    def _sm(v, p):
        s = sum(v[:p]); r = [s]
        for x in v[p:]: s = s - s/p + x; r.append(s)
        return r
    st=_sm(trs,period); sp=_sm(pdms,period); sm=_sm(mdms,period)
    if not st or st[-1]==0: return None, None, None
    pdi=100*sp[-1]/st[-1]; mdi=100*sm[-1]/st[-1]
    dx = [100*abs(100*p/t - 100*m/t)/(100*p/t + 100*m/t)
          for p,m,t in zip(sp,sm,st) if t and (100*p/t + 100*m/t)]
    return (sum(dx[-period:])/period if len(dx)>=period else None), pdi, mdi

def _bull_bear_power(candles, period=13):
    if len(candles) < period: return None, None
    ev = _ema([c['close'] for c in candles], period)
    if ev is None: return None, None
    return candles[-1]['high'] - ev, candles[-1]['low'] - ev

def _momentum(closes, period=20):
    if len(closes) < period + 1: return None
    return closes[-1] - closes[-(period+1)]

def _ppo(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal: return None, None, None
    ef=_ema(closes,fast); es=_ema(closes,slow)
    if not ef or not es or es==0: return None, None, None
    ppo = ((ef-es)/es)*100
    series = [((ef2-es2)/es2)*100 for i in range(slow-1, len(closes))
              for ef2,es2 in [(_ema(closes[:i+1],fast), _ema(closes[:i+1],slow))]
              if ef2 and es2 and es2!=0]
    if len(series) < signal: return ppo, None, None
    sig = _ema(series, signal)
    return ppo, sig, (ppo-sig) if sig else None

def _stoch_rsi(closes, period=14, sk=3, sd=3):
    if len(closes) < period*2+sk+sd: return None, None
    rs = [_rsi(closes[:i], period) for i in range(period, len(closes)+1)]
    if len(rs) < period: return None, None
    st = []
    for i in range(period-1, len(rs)):
        w=rs[i-period+1:i+1]; mn=min(w); mx=max(w)
        st.append(((rs[i]-mn)/(mx-mn)*100) if (mx-mn) else 50.0)
    if len(st) < sk+sd: return None, None
    ks = [sum(st[i-sk+1:i+1])/sk for i in range(sk-1, len(st))]
    return (ks[-1], sum(ks[-sd:])/sd) if len(ks)>=sd else (None, None)

def _ichimoku(candles, t=9, k=26, sb=52):
    if len(candles) < sb: return None
    def _m(sl): return (max(c['high'] for c in sl)+min(c['low'] for c in sl))/2
    tn=_m(candles[-t:]); kj=_m(candles[-k:])
    return {'tenkan':tn,'kijun':kj,'senkou_a':(tn+kj)/2,'senkou_b':_m(candles[-sb:]),
            'chikou':candles[-1]['close'],
            'price_ago':candles[-k]['close'] if len(candles)>=k else None}

def _cci(candles, period=20):
    if len(candles) < period: return None
    rc=candles[-period:]; tp=[(c['high']+c['low']+c['close'])/3 for c in rc]
    sma=sum(tp)/period; md=sum(abs(t-sma) for t in tp)/period
    return 0 if md==0 else (tp[-1]-sma)/(0.015*md)

def _awesome_oscillator(candles, fast=5, slow=34):
    if len(candles) < slow: return None
    mids=[(c['high']+c['low'])/2 for c in candles]
    sf=_sma(mids,fast); ss=_sma(mids,slow)
    return (sf-ss) if (sf and ss) else None

def _williams_r(candles, period=14):
    if len(candles) < period: return None
    rc=candles[-period:]; hh=max(c['high'] for c in rc); ll=min(c['low'] for c in rc)
    close=candles[-1]['close']
    return -50.0 if hh==ll else ((hh-close)/(hh-ll))*-100

def _ma_suite_score(closes, label=""):
    sb, ss, det = 0, 0, []
    price = closes[-1]
    for p in [5, 10, 20, 50, 100, 200]:
        for fn, nm in [(_sma, 'SMA'), (_ema, 'EMA')]:
            v = fn(closes, p)
            if v:
                if price > v: sb+=1; det.append(f"[{label}] Price>{nm}{p} → BUY +1")
                else:         ss+=1; det.append(f"[{label}] Price<{nm}{p} → SELL +1")
    return sb, ss, det


def _score_timeframe(candles, label=""):
    if len(candles) < 30:
        return {'bias':'NEUTRAL','score':0,'details':[],'score_buy':0,'score_sell':0,'net':0,
                'rsi':50,'ema9':None,'ema21':None,'ema50':None,'adx':None,'cci':None,
                'ao':None,'williams_r':None,'uo':None,'ppo':None,'stoch_k':None,'hma':None}

    closes=[c['close'] for c in candles]; sb=0; ss=0; det=[]

    e9=_ema(closes,9); e21=_ema(closes,21)
    if e9 and e21:
        if e9>e21: sb+=2; det.append(f"[{label}] EMA9>EMA21 → BUY +2")
        else:      ss+=2; det.append(f"[{label}] EMA9<EMA21 → SELL +2")

    e50=_ema(closes,50) if len(closes)>=50 else None
    if e21 and e50:
        if e21>e50: sb+=1; det.append(f"[{label}] EMA21>EMA50 → BUY +1")
        else:       ss+=1; det.append(f"[{label}] EMA21<EMA50 → SELL +1")

    rsi=_rsi(closes,14)
    if   rsi<30: sb+=3; det.append(f"[{label}] RSI={rsi:.1f} OVERSOLD → BUY +3")
    elif rsi>70: ss+=3; det.append(f"[{label}] RSI={rsi:.1f} OVERBOUGHT → SELL +3")
    elif rsi<45: sb+=1; det.append(f"[{label}] RSI={rsi:.1f} <45 → BUY +1")
    elif rsi>55: ss+=1; det.append(f"[{label}] RSI={rsi:.1f} >55 → SELL +1")

    ml,sl_,hist=_macd(closes)
    if hist is not None and ml and sl_:
        thr=abs(closes[-1])*0.00005
        if   abs(hist)>thr and hist>0 and ml>sl_: sb+=3; det.append(f"[{label}] MACD strong BUY → +3")
        elif abs(hist)>thr and hist<0 and ml<sl_: ss+=3; det.append(f"[{label}] MACD strong SELL → +3")
        elif ml>sl_: sb+=1; det.append(f"[{label}] MACD>signal → BUY +1")
        elif ml<sl_: ss+=1; det.append(f"[{label}] MACD<signal → SELL +1")

    bbu,_,bbl=_bollinger(closes)
    if bbu and bbl:
        rng=bbu-bbl
        if rng>0:
            pos=(closes[-1]-bbl)/rng
            if   pos<0.15: sb+=2; det.append(f"[{label}] BB lower extreme → BUY +2")
            elif pos>0.85: ss+=2; det.append(f"[{label}] BB upper extreme → SELL +2")

    cs=_candle_strength(candles,5)
    if cs==1:  sb+=1; det.append(f"[{label}] Bullish candles → BUY +1")
    elif cs==-1:ss+=1; det.append(f"[{label}] Bearish candles → SELL +1")

    hma=_hma(closes,9)
    if hma:
        if closes[-1]>hma: sb+=1; det.append(f"[{label}] Price>HMA9 → BUY +1")
        else:               ss+=1; det.append(f"[{label}] Price<HMA9 → SELL +1")

    uo=_ultimate_oscillator(candles,7,14,28)
    if uo:
        if   uo<30: sb+=2; det.append(f"[{label}] UO={uo:.1f} OVERSOLD → BUY +2")
        elif uo>70: ss+=2; det.append(f"[{label}] UO={uo:.1f} OVERBOUGHT → SELL +2")
        elif uo>50: sb+=1; det.append(f"[{label}] UO={uo:.1f} >50 → BUY +1")
        else:       ss+=1; det.append(f"[{label}] UO={uo:.1f} <50 → SELL +1")

    adx,pdi,mdi=_adx(candles,14)
    if adx and pdi and mdi and adx>25:
        if pdi>mdi: sb+=2; det.append(f"[{label}] ADX={adx:.1f} DI+ → BUY +2")
        else:       ss+=2; det.append(f"[{label}] ADX={adx:.1f} DI- → SELL +2")

    bp,brp=_bull_bear_power(candles,13)
    if bp is not None and brp is not None:
        if   bp>0 and brp>0: sb+=2; det.append(f"[{label}] Both powers>0 → BUY +2")
        elif bp<0 and brp<0: ss+=2; det.append(f"[{label}] Both powers<0 → SELL +2")
        elif bp>0:            sb+=1; det.append(f"[{label}] Bull power>0 → BUY +1")
        elif brp<0:           ss+=1; det.append(f"[{label}] Bear power<0 → SELL +1")

    mom=_momentum(closes,20)
    if mom is not None:
        if mom>0: sb+=1; det.append(f"[{label}] Momentum+ → BUY +1")
        else:     ss+=1; det.append(f"[{label}] Momentum- → SELL +1")

    ppo,psig,phist=_ppo(closes,12,26,9)
    if ppo is not None:
        if ppo>0:   sb+=1; det.append(f"[{label}] PPO>0 → BUY +1")
        else:       ss+=1; det.append(f"[{label}] PPO<0 → SELL +1")
        if phist:
            if phist>0: sb+=1; det.append(f"[{label}] PPO hist+ → BUY +1")
            else:       ss+=1; det.append(f"[{label}] PPO hist- → SELL +1")

    sk_v,sd_v=_stoch_rsi(closes,14)
    if sk_v is not None:
        if   sk_v<20: sb+=2; det.append(f"[{label}] StochRSI OVERSOLD → BUY +2")
        elif sk_v>80: ss+=2; det.append(f"[{label}] StochRSI OVERBOUGHT → SELL +2")
        elif sd_v and sk_v>sd_v: sb+=1; det.append(f"[{label}] StochRSI K>D → BUY +1")
        elif sd_v and sk_v<sd_v: ss+=1; det.append(f"[{label}] StochRSI K<D → SELL +1")

    ichi=_ichimoku(candles,9,26,52)
    if ichi:
        price=closes[-1]; ct=max(ichi['senkou_a'],ichi['senkou_b']); cb=min(ichi['senkou_a'],ichi['senkou_b'])
        if   price>ct: sb+=2; det.append(f"[{label}] Ichimoku above cloud → BUY +2")
        elif price<cb: ss+=2; det.append(f"[{label}] Ichimoku below cloud → SELL +2")
        if ichi['tenkan']>ichi['kijun']: sb+=1; det.append(f"[{label}] Tenkan>Kijun → BUY +1")
        else:                             ss+=1; det.append(f"[{label}] Tenkan<Kijun → SELL +1")
        if ichi['price_ago']:
            if ichi['chikou']>ichi['price_ago']: sb+=1; det.append(f"[{label}] Chikou above → BUY +1")
            else:                                 ss+=1; det.append(f"[{label}] Chikou below → SELL +1")

    cci=_cci(candles,20)
    if cci is not None:
        if   cci<-100: sb+=2; det.append(f"[{label}] CCI OVERSOLD → BUY +2")
        elif cci>100:  ss+=2; det.append(f"[{label}] CCI OVERBOUGHT → SELL +2")
        elif cci>0:    sb+=1; det.append(f"[{label}] CCI>0 → BUY +1")
        else:          ss+=1; det.append(f"[{label}] CCI<0 → SELL +1")

    ao=_awesome_oscillator(candles,5,34)
    if ao is not None:
        if ao>0: sb+=1; det.append(f"[{label}] AO>0 → BUY +1")
        else:    ss+=1; det.append(f"[{label}] AO<0 → SELL +1")

    wr=_williams_r(candles,14)
    if wr is not None:
        if   wr<-80: sb+=2; det.append(f"[{label}] W%R OVERSOLD → BUY +2")
        elif wr>-20: ss+=2; det.append(f"[{label}] W%R OVERBOUGHT → SELL +2")
        elif wr<-50: ss+=1; det.append(f"[{label}] W%R bearish → SELL +1")
        else:        sb+=1; det.append(f"[{label}] W%R bullish → BUY +1")

    mb,ms,md=_ma_suite_score(closes,label)
    sb+=mb; ss+=ms; det+=md

    net=sb-ss
    if   net>0: bias,score='BUY',sb
    elif net<0: bias,score='SELL',ss
    else:       bias,score='NEUTRAL',0

    return {'bias':bias,'score':score,'score_buy':sb,'score_sell':ss,'net':net,
            'rsi':rsi,'ema9':e9,'ema21':e21,'ema50':e50,'details':det,
            'adx':adx,'cci':cci,'ao':ao,'williams_r':wr,'uo':uo,'ppo':ppo,'stoch_k':sk_v,'hma':hma}


# >>> ADDED: formats one timeframe's result into a compact display string
# e.g. "BUY (18/15/+3)". INFO ONLY — used only for saving/showing scores,
# never used in any decision-making.
def _format_tf_result(r):
    if not r:
        return "NEUTRAL (0/0/+0)"
    return f"{r.get('bias','NEUTRAL')} ({r.get('score_buy',0)}/{r.get('score_sell',0)}/{r.get('net',0):+d})"


def _fetch_candles(symbol, resolution, num_candles):
    try:
        # >>> ADDED: '2h' and '1w' keys added below (info-only timeframes), nothing else changed
        sec={'1m':60,'3m':180,'5m':300,'15m':900,'30m':1800,'1h':3600,'2h':7200,'4h':14400,'1d':86400,'1w':604800}.get(resolution,300)
        end=int(_time.time()); start=end-(num_candles*sec)
        resp=_make_api_request('GET',f'/history/candles?resolution={resolution}&symbol={symbol}&start={start}&end={end}')
        if not resp or not resp.get('result'): print(f"⚠️ No candles {symbol}@{resolution}"); return []
        parsed=[{'open':float(c.get('open',0)),'high':float(c.get('high',0)),
                 'low':float(c.get('low',0)),'close':float(c.get('close',0)),'time':c.get('time',0)}
                for c in resp['result'] if c]
        parsed.sort(key=lambda x:x['time'])
        print(f"📊 {symbol} @ {resolution}: {len(parsed)} candles fetched")
        return parsed
    except Exception as e:
        print(f"❌ Error {resolution}: {e}"); return []

def _is_market_tradeable(candles_15m):
    if len(candles_15m) < 15: return True
    atr_val=_atr(candles_15m,14); price=candles_15m[-1]['close']
    if not atr_val or price<=0: return True
    pct=(atr_val/price)*100
    if pct<0.02: print(f"⚠️ Too flat ATR={pct:.4f}%"); return False
    print(f"✅ ATR OK: {pct:.4f}%"); return True


def _print_extra_timeframe_scores(symbol):
    """
    INFO ONLY — extra timeframe scores for display (1M, 5M, 30M, 2H, 1D, 1W).
    This function does NOT make any trading decision and does NOT affect
    the strategy/entry logic in generate_smart_signal() in any way.
    Returns a dict {label: result_dict} so callers can also SAVE this data
    (e.g. to DB) without changing what gets printed.
    """
    tf_map = [
        ('1m',  '1M'),
        ('5m',  '5M'),
        ('30m', '30M'),
        ('2h',  '2H'),
        ('1d',  '1D'),
        ('1w',  '1W'),
    ]
    print(f"\n📊 EXTRA TIMEFRAME SCORES (INFO ONLY):")
    extra_results = {}
    for res, label in tf_map:
        try:
            candles = _fetch_candles(symbol, res, 120)
            if len(candles) < 20:
                print(f"   {label:4s} → NEUTRAL | Not enough candles")
                extra_results[label] = {'bias': 'NEUTRAL', 'score_buy': 0, 'score_sell': 0, 'net': 0}
                continue
            r = _score_timeframe(candles, label)
            print(f"   {label:4s} → {r['bias']:7s} | BUY={r['score_buy']:2d} SELL={r['score_sell']:2d} Net={r['net']:+3d}")
            extra_results[label] = r
        except Exception as e:
            print(f"   {label:4s} → ERROR: {e}")
            extra_results[label] = {'bias': 'NEUTRAL', 'score_buy': 0, 'score_sell': 0, 'net': 0}
    return extra_results


def generate_smart_signal(reason="trade_decision"):
    """
    SIGNAL ENGINE v7

    RULES:
    ┌─────────────────────────────────────────────────┐
    │  4H = MASTER DIRECTION                          │
    │  15M must agree with 4H + net >= 4              │
    │  1H = confidence only (not a blocker)           │
    │                                                 │
    │  4H SELL + 15M SELL + net>=4  →  SELL ✅        │
    │  4H BUY  + 15M BUY  + net>=4  →  BUY  ✅        │
    │  4H SELL + 15M BUY            →  WAIT ⏳        │
    │  4H BUY  + 15M SELL           →  WAIT ⏳        │
    │  4H NEUTRAL: need 1H+15M agree + net>=5         │
    └─────────────────────────────────────────────────┘
    """

    symbol = _get_symbol()
    print(f"\n{'='*60}")
    print(f"🧠 SIGNAL ENGINE v7 — {symbol} — {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*60}")

    candles_4h  = _fetch_candles(symbol, '4h',  120)
    candles_1h  = _fetch_candles(symbol, '1h',  120)
    candles_15m = _fetch_candles(symbol, '15m', 120)

    if len(candles_4h) < 20:  candles_4h = None
    if len(candles_1h) < 20 or len(candles_15m) < 20:
        d = random.choice(['BUY','SELL'])
        return _make_signal(d, 50, 'RANDOM_FALLBACK', 0, reason, {}, {}, {}, candles_15m or [])

    if not _is_market_tradeable(candles_15m):
        return _make_wait_signal(reason, "Market too flat")

    r4h  = _score_timeframe(candles_4h,  '4H') if candles_4h else None
    r1h  = _score_timeframe(candles_1h,  '1H')
    r15m = _score_timeframe(candles_15m, '15M')

    b4h  = r4h['bias']  if r4h  else 'NEUTRAL'
    b1h  = r1h['bias']
    b15m = r15m['bias']

    print(f"\n📊 SCORES:")
    if r4h:
        print(f"   4H  → {b4h:7s} | BUY={r4h['score_buy']:2d} SELL={r4h['score_sell']:2d} Net={r4h['net']:+3d}  ← MASTER")
    print(f"   1H  → {b1h:7s} | BUY={r1h['score_buy']:2d} SELL={r1h['score_sell']:2d} Net={r1h['net']:+3d}  (confidence only)")
    print(f"   15M → {b15m:7s} | BUY={r15m['score_buy']:2d} SELL={r15m['score_sell']:2d} Net={r15m['net']:+3d}  ← ENTRY")
    

    # >>> ADDED: single call, info-only — prints extra timeframe scores (1m,5m,30m,2h,1d,1w)
    # AND captures them so we can save a full score snapshot to DB later.
    # Does not change any variable used below in the decision logic.
    extra_scores = _print_extra_timeframe_scores(symbol)

    # >>> ADDED: build formatted timeframe_scores dict — INFO ONLY, purely for
    # saving/display. Not used anywhere in the decision logic below.
    timeframe_scores = {
        '1m':  _format_tf_result(extra_scores.get('1M')),
        '5m':  _format_tf_result(extra_scores.get('5M')),
        '15m': _format_tf_result(r15m),
        '30m': _format_tf_result(extra_scores.get('30M')),
        '1h':  _format_tf_result(r1h),
        '2h':  _format_tf_result(extra_scores.get('2H')),
        '4h':  _format_tf_result(r4h),
        '1d':  _format_tf_result(extra_scores.get('1D')),
        '1w':  _format_tf_result(extra_scores.get('1W')),
    }

    # Final decision uses ONLY user-selected timeframes (Decision Timeframes card).
    # Scoring / snapshot above stays unchanged. Bot loop / place_order untouched.
    score_map = {
        '1m':  extra_scores.get('1M'),
        '5m':  extra_scores.get('5M'),
        '15m': r15m,
        '30m': extra_scores.get('30M'),
        '1h':  r1h,
        '2h':  extra_scores.get('2H'),
        '4h':  r4h,
        '1d':  extra_scores.get('1D'),
    }
    direction, wait_resp = _resolve_signal_from_selected_tfs(score_map, timeframe_scores, reason)
    if wait_resp is not None:
        return wait_resp

    MAX_SCORE = 75.0
    w_4h = 0.35 if r4h else 0.0
    w_1h = 0.30
    w_15 = 0.35

    score_4h = r4h['score'] if r4h else 0
    conf_raw = (score_4h * w_4h + r1h['score'] * w_1h + r15m['score'] * w_15) / MAX_SCORE

    if b1h == direction:
        conf_raw = min(conf_raw * 1.15, 1.0)
        print(f"   ✅ 1H also agrees ({b1h}) — confidence boosted")
    else:
        conf_raw = conf_raw * 0.90
        print(f"   ⚠️ 1H disagrees ({b1h}) — slight confidence reduction")

    confidence = int(min(50 + conf_raw * 50, 95))

    net15 = abs(r15m['net'])
    if   net15 >= 15: layer = 'STRONG_BUY'   if direction=='BUY' else 'STRONG_SELL'
    elif net15 >= 8:  layer = 'MODERATE_BUY' if direction=='BUY' else 'MODERATE_SELL'
    else:             layer = 'WEAK_BUY'      if direction=='BUY' else 'WEAK_SELL'

    print(f"   Confidence={confidence}%  Layer={layer}")

    return _make_signal(direction, confidence, layer,
                        r15m['net'], reason, r4h or {}, r1h, r15m, candles_15m,
                        timeframe_scores=timeframe_scores)


def _make_signal(direction, confidence, layer, net_score, reason,
                 r4h, r1h, r15m, candles_15m, timeframe_scores=None):
    price=candles_15m[-1]['close'] if candles_15m else 0
    atr_val=_atr(candles_15m,14) if candles_15m else None
    if atr_val and price:
        ref_sl=round(price-(1.5*atr_val),4) if direction=='BUY' else round(price+(1.5*atr_val),4)
        ref_tp=round(price+(3.0*atr_val),4) if direction=='BUY' else round(price-(3.0*atr_val),4)
    else: ref_sl=ref_tp=None
    print(f"   📍 Entry={price} SL={ref_sl} TP={ref_tp}")

    dec_parts = []
    if r4h:
        dec_parts.append(f"4H: {r4h.get('bias','NEUTRAL')} (B={r4h.get('score_buy',0)},S={r4h.get('score_sell',0)},N={r4h.get('net',0):+d})")
    else:
        dec_parts.append("4H: NEUTRAL (B=0,S=0,N=0)")
        
    if r1h:
        dec_parts.append(f"1H: {r1h.get('bias','NEUTRAL')} (B={r1h.get('score_buy',0)},S={r1h.get('score_sell',0)},N={r1h.get('net',0):+d})")
    else:
        dec_parts.append("1H: NEUTRAL (B=0,S=0,N=0)")
        
    if r15m:
        dec_parts.append(f"15M: {r15m.get('bias','NEUTRAL')} (B={r15m.get('score_buy',0)},S={r15m.get('score_sell',0)},N={r15m.get('net',0):+d})")
    else:
        dec_parts.append("15M: NEUTRAL (B=0,S=0,N=0)")
        
    trade_decisions_str = " | ".join(dec_parts)

    return {
        'signal':direction,'timestamp':datetime.now().isoformat(),
        'confidence':confidence,'layer':layer,'score':net_score,
        'score_buy':r15m.get('score_buy',0),'score_sell':r15m.get('score_sell',0),
        'source':'smart_signal_v7','entry_price':price,'ref_sl':ref_sl,'ref_tp':ref_tp,
        'reason':f"4H={r4h.get('bias','?')} 1H={r1h.get('bias','?')} 15M={r15m.get('bias','?')} Net={net_score}",
        'trade_decisions': trade_decisions_str,
        'decision_ready':True,'decision_confidence':confidence/100,'wait':False,
        'position_analysis':{'has_position':False},
        'backtest_results':{
            'ema9':r15m.get('ema9'),'ema21':r15m.get('ema21'),'ema50':r15m.get('ema50'),
            'rsi':r15m.get('rsi'),'adx':r15m.get('adx'),'cci':r15m.get('cci'),
            'ao':r15m.get('ao'),'williams_r':r15m.get('williams_r'),'uo':r15m.get('uo'),
            'ppo':r15m.get('ppo'),'stoch_k':r15m.get('stoch_k'),'hma':r15m.get('hma'),
            'price':price,'ref_sl':ref_sl,'ref_tp':ref_tp,'factors':r15m.get('details',[]),
            '4h_bias':r4h.get('bias','?'),'1h_bias':r1h.get('bias','?'),'15m_bias':r15m.get('bias','?'),
        },
        'timeframe_scores': timeframe_scores or {},   # >>> ADDED: info only
        'last_trade_result':reason,
    }

def _make_wait_signal(reason, why, timeframe_scores=None):
    print(f"⏸️ WAIT: {why}")
    return {
        'signal':'WAIT','timestamp':datetime.now().isoformat(),
        'confidence':0,'layer':'WAIT','score':0,'score_buy':0,'score_sell':0,
        'source':'smart_signal_v7','reason':why,'entry_price':None,'ref_sl':None,'ref_tp':None,
        'decision_ready':False,'decision_confidence':0,'wait':True,
        'position_analysis':{'has_position':False},'backtest_results':{},
        'timeframe_scores': timeframe_scores or {},   # >>> ADDED: info only
        'last_trade_result':reason,
    }


# ========== Decision TF confluence (uses UI-selected TFs from app) ==========
def _decision_tf_bias_net(score_map, key):
    r = score_map.get(key)
    if not r:
        return 'NEUTRAL', 0
    return r.get('bias', 'NEUTRAL'), int(r.get('net', 0) or 0)


def _resolve_signal_from_selected_tfs(score_map, timeframe_scores, reason):
    """
    Final decision gate using only user-selected timeframes.
    Does not place orders / touch bot loop — only returns signal or WAIT.
    Returns (direction_or_None, wait_response_or_None).
    """
    tf_cfg = _get_decision_tf_config()
    enabled = [t for t in _decision_tf_priority() if tf_cfg.get(t)]
    print(f"\n🎯 DECISION TFs (saved): {[t.upper() for t in enabled] or ['(none)']}")

    if not enabled:
        return None, _make_wait_signal(
            reason,
            "No decision timeframes enabled — save at least one in Decision Timeframes",
            timeframe_scores=timeframe_scores
        )

    master = None
    master_tf = None
    for t in enabled:
        bias, _ = _decision_tf_bias_net(score_map, t)
        if bias in ('BUY', 'SELL'):
            master, master_tf = bias, t
            break

    if not master:
        return None, _make_wait_signal(
            reason,
            f"All selected TFs NEUTRAL ({', '.join(t.upper() for t in enabled)})",
            timeframe_scores=timeframe_scores
        )

    print(f"   Master = {master_tf.upper()} → {master}")

    mismatched = []
    for t in enabled:
        bias, _ = _decision_tf_bias_net(score_map, t)
        if bias != master:
            mismatched.append(f"{t.upper()}={bias}")
    if mismatched:
        why = f"Waiting for {', '.join(mismatched)} to align with {master_tf.upper()} {master}"
        print(f"⏳ Confluence fail — {why}")
        return None, _make_wait_signal(reason, why, timeframe_scores=timeframe_scores)

    # Strength: prefer 15M if selected, else lowest selected TF
    if '15m' in enabled:
        entry_tf = '15m'
        min_net = 4
    else:
        entry_tf = enabled[-1]  # lowest priority among enabled
        min_net = 4
    _, entry_net = _decision_tf_bias_net(score_map, entry_tf)
    if abs(entry_net) < min_net:
        why = f"{entry_tf.upper()} net={entry_net} too weak (need >={min_net})"
        print(f"⏳ {why}")
        return None, _make_wait_signal(reason, why, timeframe_scores=timeframe_scores)

    aligned = '+'.join(t.upper() for t in enabled)
    print(f"\n✅ {aligned} aligned on {master} — SIGNAL: {master}")
    return master, None
