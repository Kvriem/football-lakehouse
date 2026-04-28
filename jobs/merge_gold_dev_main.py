import requests

NESSIE_URL = "http://nessie:19120/api/v1"

def merge_branch(from_ref: str, into_ref: str):
    """
    Merge from_ref -> into_ref using Nessie REST API.
    Works from notebook/PySpark environment (no shell needed).
    """
    url = f"{NESSIE_URL}/trees/branch/{into_ref}/merge"
    payload = {"fromRefName": from_ref}

    r = requests.post(url, json=payload, timeout=60)

    if r.status_code not in (200, 201):
        raise RuntimeError(
            f"Merge failed {from_ref} -> {into_ref}: "
            f"status={r.status_code}, body={r.text}"
        )

    print(f"Merge succeeded: {from_ref} -> {into_ref}")
    try:
        print(r.json())
    except Exception:
        print("No JSON body returned.")

# Step 1: gold_dev -> dev
merge_branch("gold_dev", "dev")

# Step 2: dev -> main
merge_branch("dev", "main")