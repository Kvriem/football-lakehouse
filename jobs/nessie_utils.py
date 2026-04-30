"""Nessie REST helpers for branch lifecycle and merging.

Used by Airflow tasks and Spark job entrypoints so that branch creation
and promotion logic stays out of business code.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional
from urllib import error, parse, request
import json


DEFAULT_NESSIE_URL = os.environ.get("NESSIE_URL", "http://nessie:19120/api/v1")

logger = logging.getLogger(__name__)


def _get_branch_hash(nessie_url: str, branch: str) -> Optional[str]:
    refs = _http_json("GET", f"{nessie_url}/trees", timeout=30).get("references", [])
    match = next(
        (r for r in refs if r.get("type") == "BRANCH" and r.get("name") == branch),
        None,
    )
    return match["hash"] if match else None


def ensure_branch(
    target: str,
    base: str = "main",
    nessie_url: str = DEFAULT_NESSIE_URL,
) -> str:
    """Create `target` branch from `base` if missing. Returns target's commit hash."""
    base_hash = _get_branch_hash(nessie_url, base)
    if base_hash is None:
        raise RuntimeError(f"Base branch {base!r} not found in Nessie at {nessie_url}.")

    target_hash = _get_branch_hash(nessie_url, target)
    if target_hash is not None:
        logger.info("Branch %r already exists (%s)", target, target_hash[:12])
        return target_hash

    payload = {"type": "BRANCH", "name": target, "hash": base_hash}
    try:
        _http_json(
            "POST",
            f"{nessie_url}/trees/tree?{parse.urlencode({'sourceRefName': base})}",
            payload=payload,
            timeout=30,
        )
    except RuntimeError:
        # Idempotency under concurrent creators: if branch now exists, treat as success.
        raced_hash = _get_branch_hash(nessie_url, target)
        if raced_hash is not None:
            logger.info(
                "Branch %r appeared concurrently during create (%s)",
                target,
                raced_hash[:12],
            )
            return raced_hash
        raise

    new_hash = _get_branch_hash(nessie_url, target)
    if new_hash is None:
        raise RuntimeError(f"Branch {target!r} not visible after creation.")
    logger.info("Created branch %r (%s) from %r", target, new_hash[:12], base)
    return new_hash


def merge_branch(
    from_ref: str,
    into_ref: str,
    nessie_url: str = DEFAULT_NESSIE_URL,
) -> None:
    """Merge `from_ref` into `into_ref` via Nessie REST.

    Newer Nessie versions require `fromHash` in the merge payload.
    """
    url = f"{nessie_url}/trees/branch/{into_ref}/merge"
    from_hash = _get_branch_hash(nessie_url, from_ref)
    if from_hash is None:
        raise RuntimeError(f"Merge source branch {from_ref!r} not found at {nessie_url}.")
    into_hash = _get_branch_hash(nessie_url, into_ref)
    if into_hash is None:
        raise RuntimeError(f"Merge target branch {into_ref!r} not found at {nessie_url}.")

    _http_json(
        "POST",
        f"{url}?{parse.urlencode({'expectedHash': into_hash})}",
        payload={
            "fromRefName": from_ref,
            "fromHash": from_hash,
        },
        timeout=60,
    )
    logger.info("Merged %s -> %s", from_ref, into_ref)


def _http_json(
    method: str,
    url: str,
    payload: Optional[dict[str, Any]] = None,
    timeout: int = 30,
) -> dict[str, Any]:
    parsed = parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"Unsupported URL scheme in {url!r}")

    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url=url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8").strip()
            if not body:
                return {}
            return json.loads(body)
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Nessie API {method} {url} failed: status={exc.code} body={body}"
        ) from exc
    except error.URLError as exc:
        raise RuntimeError(f"Nessie API {method} {url} failed: {exc}") from exc
