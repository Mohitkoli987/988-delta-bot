"""
Lazy bot strategy loader.

Only the bot the user selected is imported into memory.
Switching bots unloads the previous module from sys.modules so RAM is freed.
"""

import importlib
import sys
from threading import Lock

AVAILABLE_BOTS = (
    {"id": "bot1", "module": "bot1", "label": "Bot 1 — Signal Engine v7", "default_ready": True},
    {"id": "bot2", "module": "bot2", "label": "Bot 2 — (empty)", "default_ready": False},
    {"id": "bot3", "module": "bot3", "label": "Bot 3 — (empty)", "default_ready": False},
    {"id": "bot4", "module": "bot4", "label": "Bot 4 — (empty)", "default_ready": False},
)

_lock = Lock()
_active_bot_id = None
_active_module = None
_runtime_factory = None  # callable set by app.py → returns runtime dict


def set_runtime_factory(factory):
    """app.py calls this once at startup with a lambda that builds runtime deps."""
    global _runtime_factory
    _runtime_factory = factory


def list_bots():
    """
    Metadata for UI.
    Briefly peeks each module for BOT_READY/BOT_NAME then unloads non-active ones
    so RAM is not held after the list call.
    """
    items = []
    for meta in AVAILABLE_BOTS:
        ready = bool(meta.get("default_ready", False))
        name = meta["label"]
        try:
            if _active_bot_id == meta["id"] and _active_module is not None:
                ready = bool(getattr(_active_module, "BOT_READY", False))
                name = getattr(_active_module, "BOT_NAME", meta["label"])
            else:
                mod = importlib.import_module(meta["module"])
                ready = bool(getattr(mod, "BOT_READY", False))
                name = getattr(mod, "BOT_NAME", meta["label"])
                _unload_module(meta["module"])
        except Exception:
            pass
        items.append({
            "id": meta["id"],
            "name": name,
            "ready": ready,
            "active": meta["id"] == _active_bot_id,
        })
    return items


def get_active_bot_id():
    return _active_bot_id


def _unload_module(module_name):
    sys.modules.pop(module_name, None)


def activate_bot(bot_id):
    """
    Load exactly one bot module, bind runtime, unload any previous bot.
    Returns (ok: bool, message: str, bot_meta: dict|None)
    """
    global _active_bot_id, _active_module

    meta = next((b for b in AVAILABLE_BOTS if b["id"] == bot_id), None)
    if not meta:
        return False, f"Unknown bot: {bot_id}", None
    if _runtime_factory is None:
        return False, "Bot runtime not configured", None

    with _lock:
        # Unload previous strategy so only one lives in RAM
        if _active_module is not None and _active_bot_id and _active_bot_id != bot_id:
            prev = next((b for b in AVAILABLE_BOTS if b["id"] == _active_bot_id), None)
            _active_module = None
            if prev:
                _unload_module(prev["module"])
            _active_bot_id = None

        # Fresh import (reload if same bot re-activated after code edit)
        _unload_module(meta["module"])
        mod = importlib.import_module(meta["module"])
        if not getattr(mod, "BOT_READY", False):
            _unload_module(meta["module"])
            return False, f"{meta['label']} has no strategy yet (BOT_READY=False)", None

        if not hasattr(mod, "generate_smart_signal"):
            _unload_module(meta["module"])
            return False, f"{bot_id} missing generate_smart_signal()", None

        runtime = _runtime_factory()
        if hasattr(mod, "bind_runtime"):
            mod.bind_runtime(runtime)

        _active_module = mod
        _active_bot_id = bot_id
        name = getattr(mod, "BOT_NAME", meta["label"])
        return True, f"Activated {name}", {
            "id": bot_id,
            "name": name,
            "ready": True,
        }


def deactivate_bot():
    """Unload active strategy module (call on stop if you want to free RAM)."""
    global _active_bot_id, _active_module
    with _lock:
        if _active_bot_id:
            prev = next((b for b in AVAILABLE_BOTS if b["id"] == _active_bot_id), None)
            _active_module = None
            if prev:
                _unload_module(prev["module"])
            _active_bot_id = None


def generate_smart_signal(reason="trade_decision"):
    """Delegate to the currently activated bot only."""
    with _lock:
        mod = _active_module
        bot_id = _active_bot_id
    if mod is None or not hasattr(mod, "generate_smart_signal"):
        from datetime import datetime
        return {
            "signal": "WAIT",
            "timestamp": datetime.now().isoformat(),
            "confidence": 0,
            "layer": "WAIT",
            "score": 0,
            "score_buy": 0,
            "score_sell": 0,
            "source": "bot_manager",
            "reason": "No bot activated — select a bot in UI before start",
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
            "active_bot": bot_id,
        }
    return mod.generate_smart_signal(reason=reason)
