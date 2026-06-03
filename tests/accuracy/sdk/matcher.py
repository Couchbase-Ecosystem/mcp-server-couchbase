"""Flexible parameter matchers for the accuracy scorer.

The scorer compares expected vs actual tool parameters. Plain literal values
must compare equal, but real-world LLM output is non-deterministic, so we
provide a set of Matcher subclasses that express softer rules (any value,
case-insensitive string, "either undefined or one of these", etc.).

Each ``match(actual)`` call returns a similarity score in ``{0, 0.75, 1.0}``
following the same scoring rubric as the top-level accuracy score:
  - 1.0 = perfect match
  - 0.75 = partial match (e.g. extra keys present)
  - 0 = no match
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any


class Matcher(ABC):
    """Base class for all matchers.

    The static factory methods (e.g. ``Matcher.any_value``) are the intended
    public API — they read naturally inside test definitions.
    """

    @abstractmethod
    def match(self, actual: Any) -> float:
        """Return a similarity score in {0, 0.75, 1.0}."""

    # ----- factory helpers -----------------------------------------------

    @staticmethod
    def value(expected: Any) -> Matcher:
        if isinstance(expected, Matcher):
            return expected
        return ValueMatcher(expected)

    @staticmethod
    def any_value() -> Matcher:
        return AnyValueMatcher()

    @staticmethod
    def empty_object_or_undefined() -> Matcher:
        return EmptyObjectOrUndefinedMatcher()

    @staticmethod
    def undefined() -> Matcher:
        return UndefinedMatcher()

    @staticmethod
    def null() -> Matcher:
        return NullMatcher()

    @staticmethod
    def boolean(expected: bool | None = None) -> Matcher:
        return BooleanMatcher(expected)

    @staticmethod
    def number(predicate: Callable[[float], bool] | None = None) -> Matcher:
        return NumberMatcher(predicate)

    @staticmethod
    def string(predicate: Callable[[str], bool] | None = None) -> Matcher:
        return StringMatcher(predicate)

    @staticmethod
    def case_insensitive_string(text: str) -> Matcher:
        return CaseInsensitiveStringMatcher(text)

    @staticmethod
    def any_of(*matchers: Matcher) -> Matcher:
        return CompositeMatcher(list(matchers))

    @staticmethod
    def not_(matcher: Matcher) -> Matcher:
        return NotMatcher(matcher)


class AnyValueMatcher(Matcher):
    def match(self, actual: Any) -> float:
        return 1.0


class EmptyObjectOrUndefinedMatcher(Matcher):
    def match(self, actual: Any) -> float:
        if actual is None:
            return 1.0
        if isinstance(actual, dict) and len(actual) == 0:
            return 1.0
        return 0.0


class UndefinedMatcher(Matcher):
    def match(self, actual: Any) -> float:
        return 1.0 if actual is None else 0.0


class NullMatcher(Matcher):
    def match(self, actual: Any) -> float:
        # Python has no distinct undefined; treat both None and absence as null.
        return 1.0 if actual is None else 0.0


class BooleanMatcher(Matcher):
    def __init__(self, expected: bool | None = None) -> None:
        self._expected = expected

    def match(self, actual: Any) -> float:
        if not isinstance(actual, bool):
            return 0.0
        if self._expected is None:
            return 1.0
        return 1.0 if actual is self._expected else 0.0


class NumberMatcher(Matcher):
    def __init__(self, predicate: Callable[[float], bool] | None = None) -> None:
        self._predicate = predicate or (lambda _v: True)

    def match(self, actual: Any) -> float:
        if isinstance(actual, bool):  # bools are ints in Python; exclude them
            return 0.0
        if not isinstance(actual, int | float):
            return 0.0
        return 1.0 if self._predicate(actual) else 0.0


class StringMatcher(Matcher):
    def __init__(self, predicate: Callable[[str], bool] | None = None) -> None:
        self._predicate = predicate or (lambda _v: True)

    def match(self, actual: Any) -> float:
        if not isinstance(actual, str):
            return 0.0
        return 1.0 if self._predicate(actual) else 0.0


class CaseInsensitiveStringMatcher(Matcher):
    def __init__(self, expected: str) -> None:
        self._expected = expected

    def match(self, actual: Any) -> float:
        if not isinstance(actual, str):
            return 0.0
        return 1.0 if actual.lower() == self._expected.lower() else 0.0


class CompositeMatcher(Matcher):
    """Returns 1.0 if any child matches perfectly; otherwise the best score."""

    def __init__(self, matchers: list[Matcher]) -> None:
        self._matchers = matchers

    def match(self, actual: Any) -> float:
        best = 0.0
        for matcher in self._matchers:
            score = matcher.match(actual)
            if score == 1.0:
                return 1.0
            best = max(best, score)
        return best


class NotMatcher(Matcher):
    def __init__(self, matcher: Matcher) -> None:
        self._matcher = matcher

    def match(self, actual: Any) -> float:
        return 0.0 if self._matcher.match(actual) == 1.0 else 1.0


class ValueMatcher(Matcher):
    """Default matcher for literal expected values.

    Recurses into dicts and lists. Extra keys/elements in the actual value
    are treated as a 0 (incorrect) — define matchers explicitly when extras
    should be tolerated.
    """

    def __init__(self, expected: Any) -> None:
        self._expected = expected

    def match(self, actual: Any) -> float:  # noqa: PLR0911
        expected = self._expected

        # Quick equality short-circuit
        if expected == actual:
            return 1.0

        if expected is None:
            return 1.0 if actual is None else 0.0

        if isinstance(expected, list):
            if not isinstance(actual, list):
                return 0.0
            if len(actual) > len(expected):
                return 0.0
            score = 1.0
            for exp_item, act_item in zip(expected, actual, strict=False):
                score = min(score, Matcher.value(exp_item).match(act_item))
                if score == 0.0:
                    return 0.0
            return score

        if isinstance(expected, dict):
            if not isinstance(actual, dict):
                return 0.0
            if len(actual) > len(expected):
                # Actual has unexpected extra keys
                return 0.0
            score = 1.0
            for key, exp_val in expected.items():
                score = min(score, Matcher.value(exp_val).match(actual.get(key)))
                if score == 0.0:
                    return 0.0
            return score

        # Scalar mismatch
        return 0.0
