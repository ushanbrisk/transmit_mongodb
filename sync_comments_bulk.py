from pymongo import MongoClient, ReplaceOne
import logging
import time
# from prometheus_client import start_http_server, Counter, Gauge

# ========= 日志配置 =========
logging.basicConfig(
    filename="sync_comments.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# # ========= Prometheus =========
# SYNC_BATCH = Counter(
#     "comments_sync_batch_total",
#     "Total batches synced"
# )
# SYNC_RECORDS = Counter(
#     "comments_sync_records_total",
#     "Total records synced"
# )
# LAST_ID = Gauge(
#     "comments_sync_last_id",
#     "Last synced _id"
# )

# ========= MongoDB =========
src = MongoClient("mongodb://localhost:27017")["netease"]["comments"]

dst = MongoClient("mongodb://192.168.3.8:27017")["netease"]["comments"]

BATCH_SIZE = 20000
last_id = None

logging.info("Sync started")

# # ========= Prometheus HTTP =========
# start_http_server(9100)  # http://localhost:9100/metrics
accum = 0
while True:
    query = {"_id": {"$gt": last_id}} if last_id else {}
    cursor = src.find(query).sort("_id", 1).limit(BATCH_SIZE)

    docs = list(cursor)
    if not docs:
        break

    accum += BATCH_SIZE

    ops = []
    batch_ids = []

    for c in docs:
        doc = dict(c)
        doc.pop("_id", None)

        ops.append(
            ReplaceOne(
                {"commentId": doc["commentId"]},
                doc,
                upsert=True
            )
        )
        batch_ids.append(c.get("commentId"))

    if not ops:
        break

    dst.bulk_write(ops, ordered=False)

    last_id = docs[-1]["_id"]
    # LAST_ID.set(float(str(last_id)))

    # SYNC_BATCH.inc()
    # SYNC_RECORDS.inc(len(ops))

    logging.info(
        f"Batch done, last_id={last_id}, "
        f"records={len(ops)}, "
        f"commentIds={batch_ids[:3]}..."
    )
    print(f"synced up to _id={last_id}")

logging.info("✅ Sync completed")

