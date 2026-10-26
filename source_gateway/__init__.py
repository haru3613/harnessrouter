"""Fail-closed boundary for the private HARU Dispatch compatibility runtime."""

from .validation import StateValidationError, validate_state

__all__ = ["StateValidationError", "validate_state"]
