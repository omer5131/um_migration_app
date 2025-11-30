"""Backward-compatible wrapper for recommendation engine.

The implementation lives in src.recommendation.engine. This module re-exports
MigrationLogic to avoid breaking existing imports.
"""

from src.recommendation.engine import MigrationLogic  # re-export

__all__ = ["MigrationLogic"]
