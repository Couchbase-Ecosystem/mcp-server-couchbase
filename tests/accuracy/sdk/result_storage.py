"""Persist accuracy test results to disk as JSON.

One JSON file is written per accuracy run id (``CB_ACCURACY_RUN_ID``) under
``tests/accuracy/results/``. Each run is a list of prompt entries; each
prompt entry holds the prompt text, the expected tool calls, and one
``ModelResponse`` per model tested.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from .judge import JudgeVerdict
from .matcher import Matcher
from .types import ExpectedToolCall, ModelResponse


class DiskResultStorage:
    """Append-style storage keyed by ``(commit_sha, run_id)``."""

    def __init__(self, results_dir: Path) -> None:
        self._results_dir = results_dir
        self._results_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _path(self, run_id: str) -> Path:
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in run_id)
        return self._results_dir / f"{safe}.json"

    def save_model_response(
        self,
        *,
        run_id: str,
        commit_sha: str,
        prompt: str,
        expected_tool_calls: list[ExpectedToolCall],
        model_response: ModelResponse,
    ) -> None:
        with self._lock:
            path = self._path(run_id)
            doc: dict[str, Any]
            if path.exists():
                doc = json.loads(path.read_text())
            else:
                doc = {
                    "run_id": run_id,
                    "commit_sha": commit_sha,
                    "created_on": int(time.time()),
                    "prompt_results": [],
                }

            prompt_entry = next(
                (p for p in doc["prompt_results"] if p["prompt"] == prompt),
                None,
            )
            if prompt_entry is None:
                prompt_entry = {
                    "prompt": prompt,
                    "expected_tool_calls": [
                        _serialize(call) for call in expected_tool_calls
                    ],
                    "model_responses": [],
                }
                doc["prompt_results"].append(prompt_entry)

            prompt_entry["model_responses"].append(_serialize(model_response))
            path.write_text(json.dumps(doc, indent=2, default=_default_json))

    def save_result_eval(
        self,
        *,
        run_id: str,
        commit_sha: str,
        prompt: str,
        expectation: str,
        verdict: JudgeVerdict,
        answer: str,
        tool_results: list[dict[str, Any]],
        tool_calling_accuracy: float | None,
        model_response: ModelResponse,
    ) -> None:
        """Append one LLM-as-judge result-validation outcome.

        Stored under a separate ``result_evals`` array in the same per-run
        JSON file used by ``save_model_response`` so a single run id holds
        both tool-calling and answer-correctness results.
        """
        with self._lock:
            path = self._path(run_id)
            doc: dict[str, Any]
            if path.exists():
                doc = json.loads(path.read_text())
            else:
                doc = {
                    "run_id": run_id,
                    "commit_sha": commit_sha,
                    "created_on": int(time.time()),
                    "prompt_results": [],
                }

            doc.setdefault("result_evals", [])
            doc["result_evals"].append(
                _serialize(
                    {
                        "prompt": prompt,
                        "expectation": expectation,
                        "verdict": verdict,
                        "answer": answer,
                        "tool_results": tool_results,
                        "tool_calling_accuracy": tool_calling_accuracy,
                        "model_response": model_response,
                    }
                )
            )
            path.write_text(json.dumps(doc, indent=2, default=_default_json))


def _serialize(obj: Any) -> Any:
    if is_dataclass(obj):
        return _serialize(asdict(obj))
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    return obj


def _default_json(obj: Any) -> Any:
    if isinstance(obj, Matcher):
        return f"<{type(obj).__name__}>"
    if is_dataclass(obj):
        return asdict(obj)
    return repr(obj)
