from pymongo import MongoClient


# ===== 源库（本地）=====
src_client = MongoClient("mongodb://localhost:27017")
src_db = src_client["netease"]
src_comments = src_db["comments"]


# ===== 目标库（远程）=====
dst_client = MongoClient("mongodb://192.168.3.8:27017")
dst_db = dst_client["netease"]
dst_comments = dst_db["comments"]

batch_size = 1000
last_id = None

print("Start syncing comments...")

while True:
    query = {"_id": {"$gt": last_id}} if last_id else {}

    comments = list(
        src_comments.find(query)
        .sort("_id", 1)
        .limit(batch_size)
    )

    if not comments:
        break

    for c in comments:
        c.pop("_id", None)  # 避免 _id 冲突
        dst_comments.update_one(
            {"commentId": c["commentId"]},
            {"$setOnInsert": c},
            upsert=True
        )

    last_id = comments[-1]["_id"]
    print(f"Synced up to _id = {last_id}")

print("✅ Sync completed.")


