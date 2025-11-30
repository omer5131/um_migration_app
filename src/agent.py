"""Backward-compatible shim for agent review class.

The actual implementation now lives in src.agent.review.
"""

from src.agent.review import ReviewAgent  # re-export

__all__ = ["ReviewAgent"]
