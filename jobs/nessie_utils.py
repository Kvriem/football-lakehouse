"""Nessie REST helpers for branch lifecycle and merging.

Used by Airflow tasks and Spark job entrypoints so that branch creation
and promotion logic stays out of business code.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import requests


DEFAULT_NESSIE_URL = os.environ.get("NESSIE_URL", "http://nessie:19120/api/v1")

logger = logging.getLogger(__name__)


def _get_branch_hash(nessie_url: str, branch: str) -> Optional[str]:
    refs = requests.get(f"{nessie_url}/trees", timeout=30).json().get("references", [])
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
    response = requests.post(
        f"{nessie_url}/trees/tree",
        params={"sourceRefName": base},
        json=payload,
        timeout=30,
    )
    if response.status_code not in (200, 201, 409):
        raise RuntimeError(
            f"Failed creating branch {target!r} from {base!r}: "
            f"status={response.status_code} body={response.text}"
        )

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
    """Merge `from_ref` into `into_ref` via Nessie REST."""
    url = f"{nessie_url}/trees/branch/{into_ref}/merge"
    response = requests.post(
        url,
        json={"fromRefName": from_ref},
        timeout=60,
    )
    if response.status_code not in (200, 201, 204):
        raise RuntimeError(
            f"Merge failed {from_ref} -> {into_ref}: "
            f"status={response.status_code} body={response.text}"
        )
    logger.info("Merged %s -> %s", from_ref, into_ref)
