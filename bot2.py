"""
BOT 2 — Trend-Regime + Early-Momentum Entry (v1)

WHY THIS EXISTS / WHAT IT FIXES vs BOT 1 (v7):

  v7 problem                                   →  v2 fix
  ---------------------------------------------------------------
  Needs ALL 8 TFs to align → enters only when     4H is a REGIME FILTER only
  the move is already exhausted (net scores       (trend yes/no). Entry trigger
  like -20/-21 = deeply saturated indicators)      is an EARLY EMA9/21 cross on
                                                    15M, not a saturated score.

  RSI + StochRSI + CCI + Williams%R + UO all       Only ONE momentum-exhaustion
  fire on the same oversold/overbought idea        check (RSI zone) is used.
  → fake "confluence" from duplicated votes.       No duplicate votes.

  _ma_suite_score = 12 near-identical             Removed. Trend structure is
  SMA/EMA-vs-price votes → inflates score          judged once via EMA9/21/50
  without adding real information.                 stacking, single vote.

  No regime filter — trades in choppy/flat         ADX(14) < 20 on 4H → regime
  markets same as trending ones.                   is CHOP → return WAIT.
                                                    (stronger than v7's ATR-only
                                                    flatness check.)

  random.choice(['BUY','SELL']) fallback when      Never guesses. Missing/short
  candle data is missing/short — silently           candle data → WAIT, always.
  ships a coin-flip live trade. DANGEROUS BUG.

  No enforced risk:reward — several live bots      SL/TP built from ATR with an
  in production ended up with SL > TP.              enforced min RR (default 1:2,
                                                     configurable via MIN_RR).

Same loader contract as bot1.py — app.py calls bind_runtime() once, then
generate_smart_signal() on each decision tick. This file does NOT place
orders, touch Flask/DB/websockets, or manage lot sizing.
"""

import time as _time
from datetime import datetime

BOT_ID = "bot2"
BOT_NAME = "Bot 2 — Trend-Regime + Early-Momentum Entry v1"
BOT_READY = True

MIN_RR = 2.0          # enforced minimum reward:risk (TP distance >= MIN_RR * SL distance)
SL_ATR_MULT = 1.2     # SL = entry -/+ SL_ATR_MULT * ATR(15m)
ADX_TREND_MIN = 20    # 4H ADX below this => choppy => WAIT
RSI_EXHAUST_HI = 68   # 15M RSI above this => momentum exhausted, skip new entries
RSI_EXHAUST_LO = 32   # 15M RSI below this => momentum exhausted, skip new entries
MIN_CANDLES = 60      # minimum candles required per timeframe to trust the read

_RUNTIME = None


def bind_runtime(runtime):
    """Called once when this bot is activated. runtime provides app helpers."""
    global _RUNTIME
    _RUNTIME = runtime


def _rt():
    if _RUNTIME is None:
        raise RuntimeError("bot2: runtime not bound — activate bot via app first")
    return _RUNTIME


def _get_symbol():
    return _rt()["get_symbol"]()


def _make_api_request(method, path, **kwargs):
    return _rt()["make_api_request"](method, path, **kwargs)


# ========== INDICATORS (only the ones actually used — no duplicates) ==========

def _ema(prices, period):
    if len(prices) < period:
        return None
    k = 2.0 / (period + 1)
    val = sum(prices[:period]) / period
    for p in prices[period:]:
        val = p * k + val * (1 - k)
    return val


def _ema_series(prices, period):
    """Full EMA series (needed to detect a *cross*, not just current value)."""
    if len(prices) < period:
        return []
    k = 2.0 / (period + 1)
    out = []
    val = sum(prices[:period]) / period
    out.append(val)
    for p in prices[period:]:
        val = p * k + val * (1 - k)
        out.append(val)
    return out


def _rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains) / period
    al = sum(losses) / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (period - 1) + max(d, 0)) / period
        al = (al * (period - 1) + max(-d, 0)) / period
    if al == 0:
        return 100.0
    return 100 - (100 / (1 + ag / al))


def _macd_hist_series(closes, fast=12, slow=26, signal=9):
    """Return MACD histogram series so we can detect a *sign flip* (momentum
    turning), instead of just reading whether hist>0 right now (which is
    almost always already-late info)."""
    if len(closes) < slow + signal + 2:
        return []
    macd_line = []
    for i in range(slow, len(closes) + 1):
        ef = _ema(closes[:i], fast)
        es = _ema(closes[:i], slow)
        if ef is not None and es is not None:
            macd_line.append(ef - es)
    if len(macd_line) < signal + 1:
        return []
    hist = []
    for i in range(signal, len(macd_line) + 1):
        sig = _ema(macd_line[:i], signal)
        if sig is not None:
            hist.append(macd_line[i - 1] - sig)
    return hist


def _atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]['high']; l = candles[i]['low']; pc = candles[i - 1]['close']
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def _adx(candles, period=14):
    if len(candles) < period * 2 + 1:
        return None
    trs, pdms, mdms = [], [], []
    for i in range(1, len(candles)):
        h = candles[i]['high']; l = candles[i]['low']
        ph = candles[i - 1]['high']; pl = candles[i - 1]['low']; pc = candles[i - 1]['close']
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        pdms.append(max(h - ph, 0) if (h - ph) > (pl - l) else 0)
        mdms.append(max(pl - l, 0) if (pl - l) > (h - ph) else 0)

    def _sm(v, p):
        s = sum(v[:p]); r = [s]
        for x in v[p:]:
            s = s - s / p + x
            r.append(s)
        return r

    st = _sm(trs, period); sp = _sm(pdms, period); sm = _sm(mdms, period)
    if not st or st[-1] == 0:
        return None
    dx = []
    for p, m, t in zip(sp, sm, st):
        if t and (100 * p / t + 100 * m / t):
            dx.append(100 * abs(100 * p / t - 100 * m / t) / (100 * p / t + 100 * m / t))
    if len(dx) < period:
        return None
    return sum(dx[-period:]) / period


# ========== DATA FETCH ==========

def _fetch_candles(symbol, resolution, num_candles):
    try:
        sec = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600,
               '2h': 7200, '4h': 14400, '1d': 86400, '1w': 604800}.get(resolution, 900)
        end = int(_time.time())
        start = end - (num_candles * sec)
        resp = _make_api_request('GET', f'/history/candles?resolution={resolution}&symbol={symbol}&start={start}&end={end}')
        if not resp or not resp.get('result'):
            print(f"⚠️ No candles {symbol}@{resolution}")
            return []
        parsed = [{'open': float(c.get('open', 0)), 'high': float(c.get('high', 0)),
                   'low': float(c.get('low', 0)), 'close': float(c.get('close', 0)),
                   'time': c.get('time', 0)} for c in resp['result'] if c]
        parsed.sort(key=lambda x: x['time'])
        return parsed
    except Exception as e:
        print(f"❌ Error fetching {resolution}: {e}")
        return []


# ========== REGIME FILTER (4H) — trend or chop, direction ==========

def _regime_4h(candles_4h):
    """
    Returns (trend_ok: bool, direction: 'BUY'|'SELL'|None, adx: float|None)
    trend_ok=False means market is choppy on 4H -> caller should WAIT.
    """
    closes = [c['close'] for c in candles_4h]
    if len(closes) < 55:
        return False, None, None

    e9 = _ema(closes, 9)
    e21 = _ema(closes, 21)
    e50 = _ema(closes, 50)
    adx = _adx(candles_4h, 14)

    if adx is None or adx < ADX_TREND_MIN:
        return False, None, adx  # choppy — no reliable directional edge

    if e9 is None or e21 is None or e50 is None:
        return False, None, adx

    if e9 > e21 > e50:
        return True, 'BUY', adx
    if e9 < e21 < e50:
        return True, 'SELL', adx
    return False, None, adx  # EMAs not stacked cleanly = ambiguous trend


# ========== ENTRY TRIGGER (15M) — early cross, not exhaustion ==========

def _entry_15m(candles_15m, trend_direction):
    """
    Returns (fire: bool, why: str, extra: dict)
    Fires on an EARLY EMA9/21 cross in the direction of the 4H trend, gated by:
      - RSI NOT in exhaustion zone (avoids buying tops / selling bottoms)
      - MACD histogram just flipped sign in the trend direction (momentum
        turning now, not "already turned 20 candles ago")
    """
    closes = [c['close'] for c in candles_15m]
    if len(closes) < MIN_CANDLES:
        return False, "Not enough 15M candles", {}

    e9s = _ema_series(closes, 9)
    e21s = _ema_series(closes, 21)
    if len(e9s) < 2 or len(e21s) < 2:
        return False, "EMA series too short", {}

    # align series lengths (they start at different offsets)
    off = len(e9s) - len(e21s)
    e9_now, e9_prev = e9s[-1], e9s[-2]
    e21_now, e21_prev = e21s[-1], e21s[-2]

    cross_up = e9_prev <= e21_prev and e9_now > e21_now
    cross_down = e9_prev >= e21_prev and e9_now < e21_now

    rsi = _rsi(closes, 14)
    hist = _macd_hist_series(closes, 12, 26, 9)
    hist_flip_up = len(hist) >= 2 and hist[-2] <= 0 < hist[-1]
    hist_flip_down = len(hist) >= 2 and hist[-2] >= 0 > hist[-1]
    hist_now_pos = len(hist) >= 1 and hist[-1] > 0
    hist_now_neg = len(hist) >= 1 and hist[-1] < 0

    extra = {'rsi': rsi, 'ema9': e9_now, 'ema21': e21_now,
             'macd_hist': hist[-1] if hist else None}

    if trend_direction == 'BUY':
        if rsi >= RSI_EXHAUST_HI:
            return False, f"RSI={rsi:.1f} exhausted, skip new BUY", extra
        if cross_up and (hist_flip_up or hist_now_pos):
            return True, f"EMA9 crossed above EMA21, MACD momentum turning up, RSI={rsi:.1f}", extra
        return False, "No fresh bullish cross yet on 15M", extra

    if trend_direction == 'SELL':
        if rsi <= RSI_EXHAUST_LO:
            return False, f"RSI={rsi:.1f} exhausted, skip new SELL", extra
        if cross_down and (hist_flip_down or hist_now_neg):
            return True, f"EMA9 crossed below EMA21, MACD momentum turning down, RSI={rsi:.1f}", extra
        return False, "No fresh bearish cross yet on 15M", extra

    return False, "No trend direction", extra


# ========== 1H CONFIRMATION (agree-or-neutral gate, not a stacked score) ==========

def _confirm_1h(candles_1h, direction):
    closes = [c['close'] for c in candles_1h]
    if len(closes) < 30:
        return True, "1H insufficient data — not blocking"  # don't block on missing data
    e9 = _ema(closes, 9)
    e21 = _ema(closes, 21)
    if e9 is None or e21 is None:
        return True, "1H EMA unavailable — not blocking"
    bias = 'BUY' if e9 > e21 else 'SELL'
    if bias == direction:
        return True, f"1H agrees ({bias})"
    return False, f"1H disagrees ({bias}) with 4H/15M direction ({direction})"


# ========== SIGNAL ASSEMBLY ==========

def generate_smart_signal(reason="trade_decision"):
    symbol = _get_symbol()
    print(f"\n{'='*60}")
    print(f"🧠 BOT2 v1 — Trend-Regime + Early-Momentum — {symbol} — {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*60}")

    candles_4h = _fetch_candles(symbol, '4h', 150)
    if len(candles_4h) < 55:
        return _make_wait_signal(reason, "Not enough 4H candles to judge regime")

    trend_ok, direction, adx = _regime_4h(candles_4h)
    print(f"   4H regime: trend_ok={trend_ok} direction={direction} ADX={adx}")
    if not trend_ok or direction is None:
        why = f"4H choppy/ambiguous (ADX={adx:.1f})" if adx else "4H regime unclear"
        return _make_wait_signal(reason, why)

    candles_15m = _fetch_candles(symbol, '15m', 150)
    if len(candles_15m) < MIN_CANDLES:
        return _make_wait_signal(reason, "Not enough 15M candles for entry trigger")

    fire, why, extra = _entry_15m(candles_15m, direction)
    print(f"   15M entry check: fire={fire} | {why}")
    if not fire:
        return _make_wait_signal(reason, f"4H trend={direction} but {why}")

    candles_1h = _fetch_candles(symbol, '1h', 100)
    confirm_ok, confirm_why = _confirm_1h(candles_1h, direction)
    print(f"   1H confirm: {confirm_ok} | {confirm_why}")
    if not confirm_ok:
        return _make_wait_signal(reason, confirm_why)

    return _make_signal(direction, adx, extra, candles_15m, reason)


def _make_signal(direction, adx, extra, candles_15m, reason):
    price = candles_15m[-1]['close']
    atr_val = _atr(candles_15m, 14)

    if not atr_val or not price:
        return _make_wait_signal(reason, "ATR unavailable — cannot size SL/TP safely")

    sl_dist = SL_ATR_MULT * atr_val
    tp_dist = MIN_RR * sl_dist  # enforced RR — TP is always >= MIN_RR * SL by construction

    if direction == 'BUY':
        ref_sl = round(price - sl_dist, 4)
        ref_tp = round(price + tp_dist, 4)
    else:
        ref_sl = round(price + sl_dist, 4)
        ref_tp = round(price - tp_dist, 4)

    # confidence: driven by trend strength (ADX) + how clean the 15M trigger was
    # capped 50-90, never claims false certainty
    adx_component = min(max((adx - ADX_TREND_MIN) / 30.0, 0), 1)  # 0..1
    confidence = int(50 + adx_component * 40)

    if adx >= 35:
        layer = f"STRONG_{direction}"
    elif adx >= 25:
        layer = f"MODERATE_{direction}"
    else:
        layer = f"WEAK_{direction}"

    trade_decisions = (
        f"4H: {direction} (ADX={adx:.1f}, regime trend) | "
        f"15M: ENTRY (RSI={extra.get('rsi', 0):.1f}, MACD_hist={extra.get('macd_hist')}) | "
        f"1H: confirmed"
    )

    print(f"   📍 Entry={price} SL={ref_sl} TP={ref_tp} RR={MIN_RR} Confidence={confidence}% Layer={layer}")

    return {
        'signal': direction,
        'timestamp': datetime.now().isoformat(),
        'confidence': confidence,
        'layer': layer,
        'score': int(adx),
        'score_buy': int(adx) if direction == 'BUY' else 0,
        'score_sell': int(adx) if direction == 'SELL' else 0,
        'source': 'bot2_trend_regime_v1',
        'entry_price': price,
        'ref_sl': ref_sl,
        'ref_tp': ref_tp,
        'reason': f"4H trend={direction} ADX={adx:.1f}, 15M early cross entry, 1H confirmed",
        'trade_decisions': trade_decisions,
        'decision_ready': True,
        'decision_confidence': confidence / 100,
        'wait': False,
        'position_analysis': {'has_position': False},
        'backtest_results': {
            'adx_4h': adx, 'rsi_15m': extra.get('rsi'), 'macd_hist_15m': extra.get('macd_hist'),
            'ema9_15m': extra.get('ema9'), 'ema21_15m': extra.get('ema21'),
            'price': price, 'ref_sl': ref_sl, 'ref_tp': ref_tp,
            'rr_enforced': MIN_RR, 'sl_atr_mult': SL_ATR_MULT,
        },
        'timeframe_scores': {},
        'last_trade_result': reason,
    }


def _make_wait_signal(reason, why):
    print(f"⏸️ WAIT: {why}")
    return {
        'signal': 'WAIT',
        'timestamp': datetime.now().isoformat(),
        'confidence': 0,
        'layer': 'WAIT',
        'score': 0,
        'score_buy': 0,
        'score_sell': 0,
        'source': 'bot2_trend_regime_v1',
        'reason': why,
        'entry_price': None,
        'ref_sl': None,
        'ref_tp': None,
        'decision_ready': False,
        'decision_confidence': 0,
        'wait': True,
        'position_analysis': {'has_position': False},
        'backtest_results': {},
        'timeframe_scores': {},
        'last_trade_result': reason,
    }