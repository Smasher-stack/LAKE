import os
import time
from supabase import create_client

STAGING_TABLE = os.getenv("STAGING_TABLE", "lake_readings_staging")
LIVE_TABLE = os.getenv("LIVE_TABLE", "lake_readings_live")

# Exactly 2 sends per minute => once every 30 seconds
SEND_INTERVAL_SECONDS = int(os.getenv("SEND_INTERVAL_SECONDS", "30"))

# Rows per send (every 30s). Total throughput = 2 * BATCH_SIZE per minute.
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

moved = 0

print(
    f"Replay worker started | staging={STAGING_TABLE} -> live={LIVE_TABLE} "
    f"| batch={BATCH_SIZE} | interval={SEND_INTERVAL_SECONDS}s (2 times/min)"
)

while True:
    tick_start = time.time()

    # Pull next rows from staging (oldest first)
    resp = (
        supabase.table(STAGING_TABLE)
        .select("*")
        .order("id", desc=False)
        .limit(BATCH_SIZE)
        .execute()
    )

    batch = resp.data or []
    if not batch:
        print("Staging empty. Finished ✅")
        break

    ids = [r["id"] for r in batch]

    # Prepare rows for live (remove staging id; live has created_at default now())
    live_rows = [
        {
            "node_id": r["node_id"],
            "ph": r.get("ph"),
            "turbidity": r.get("turbidity"),
            "temperature": r.get("temperature"),
            "do_level": r.get("do_level"),
        }
        for r in batch
    ]

    # Insert into live
    supabase.table(LIVE_TABLE).insert(live_rows).execute()

    # Delete from staging (MOVE semantics)
    supabase.table(STAGING_TABLE).delete().in_("id", ids).execute()

    moved += len(batch)
    print(f"Sent {len(batch)} rows | moved total={moved}")

    # Enforce 30s tick cadence
    elapsed = time.time() - tick_start
    sleep_for = max(0.0, SEND_INTERVAL_SECONDS - elapsed)
    time.sleep(sleep_for)
