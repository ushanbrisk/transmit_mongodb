from pymongo import MongoClient, ReplaceOne

src = MongoClient("mongodb://localhost:27017")["netease"]["comments"]
dst = MongoClient("mongodb://192.168.3.8:27017")["netease"]["comments"]

BATCH_SIZE = 1000
last_id = None

while True:
    cursor = src.find(
        {"_id": {"$gt": last_id}} if last_id else {}
    ).sort("_id", 1).limit(BATCH_SIZE)

    ops = []
    for c in cursor:
        c.pop("_id", None)
        ops.append(
            ReplaceOne(
                {"commentId": c["commentId"]},
                c,
                upsert=True
            )
        )

    if not ops:
        break

    dst.bulk_write(ops, ordered=False)
    last_id = cursor[0]["_id"]
    print(f"Synced up to _id = {last_id}")