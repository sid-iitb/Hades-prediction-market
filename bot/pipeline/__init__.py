"""
Bot V2 unified pipeline: types, context, registry, and execution.
All code here is greenfield; no V1 imports from pipeline.
"""
from bot.pipeline.intents import ExitAction, OrderIntent, OrderRecord

__all__ = [
    "OrderIntent",
    "ExitAction",
    "OrderRecord",
]
