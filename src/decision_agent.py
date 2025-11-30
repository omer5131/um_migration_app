"""Backward-compatible shim for decision agent.

The actual implementation now lives in src.agent.decision.
"""

from src.agent.decision import DecisionAgent  # re-export

__all__ = ["DecisionAgent"]
