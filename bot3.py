"""
BOT 3 — blank strategy slot (future)

Paste ONLY decision / strategy code here.
app.py loads this file ONLY when the user selects Bot 3 in the UI.

============================================================
AI PROMPT (copy-paste this when asking another AI for code)
============================================================

Write a complete Python trading decision module for my Flask Delta Exchange bot.
Save it as bot3.py. Follow this EXACT contract so app.py can load it:

REQUIRED EXPORTS:
  BOT_ID = "bot3"
  BOT_NAME = "Bot 3 — <short strategy name>"
  BOT_READY = True

  def bind_runtime(runtime: dict) -> None:
      # runtime keys: get_symbol, make_api_request,
      #               get_decision_tf_config, decision_tf_priority
      # Do NOT import app.py

  def generate_smart_signal(reason: str = "trade_decision") -> dict:
      # Return BUY/SELL trade dict OR WAIT dict (same shape as bot1/bot2).
      # Strategy only — no orders, Flask, DB, websockets, lot sizing.

CANDLES: GET /history/candles?resolution=...&symbol=...&start=...&end=...
See bot1.py for a full working example. See bot2.py docstring for full return shapes.
Strategy description for Bot 3: <DESCRIBE HERE>
============================================================
"""

from datetime import datetime

BOT_ID = "bot3"
BOT_NAME = "Bot 3 — (empty — paste strategy here)"
BOT_READY = False

_RUNTIME = None


def bind_runtime(runtime):
    global _RUNTIME
    _RUNTIME = runtime


def generate_smart_signal(reason="trade_decision"):
    return {
        "signal": "WAIT",
        "timestamp": datetime.now().isoformat(),
        "confidence": 0,
        "layer": "WAIT",
        "score": 0,
        "score_buy": 0,
        "score_sell": 0,
        "source": "bot3_blank",
        "reason": "Bot 3 has no strategy yet — paste code into bot3.py",
        "entry_price": None,
        "ref_sl": None,
        "ref_tp": None,
        "decision_ready": False,
        "decision_confidence": 0,
        "wait": True,
        "position_analysis": {"has_position": False},
        "backtest_results": {},
        "timeframe_scores": {},
        "last_trade_result": reason,
    }
