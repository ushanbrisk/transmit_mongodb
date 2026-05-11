from pymongo import MongoClient, ReplaceOne
from multiprocessing import Process, cpu_count
import json
import os

# ========= MongoDB =========
SRC_URI = "mongodb://localhost:27017"
DST_URI = "mongodb://192.168.3.8:27017"

SRC_DB = "netease"
SRC_COLL = "comments"
DST_DB = "netease"
DST_COLL = "comments"

BATCH_SIZE = 100000
PROCESSES = min(12, cpu_count())
RANGES_FILE = os.path.join(os.path.dirname(__file__), "comment_id_ranges.json")


# ========= Worker =========
def worker(start_comment_id, end_comment_id):
    src = MongoClient(SRC_URI)[SRC_DB][SRC_COLL]
    dst = MongoClient(DST_URI)[DST_DB][DST_COLL]

    # 每个 worker 自己查目标库获取断点
    last_doc = dst.find_one(
        {"commentId": {"$gte": start_comment_id, "$lt": end_comment_id}},
        sort=[("commentId", -1)]
    )

    if last_doc:
        last_id = last_doc["commentId"]
        print(f"[{start_comment_id}] resume from commentId {last_id}")
    else:
        last_id = start_comment_id - 1
        print(f"[{start_comment_id}] start from beginning")

    synced_count = 0
    while True:
        query = {
            "commentId": {
                "$gt": last_id,
                "$lte": end_comment_id
            }
        }

        cursor = src.find(query).sort("commentId", 1).limit(BATCH_SIZE)
        docs = list(cursor)
        if not docs:
            break

        ops = []
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

        dst.bulk_write(ops, ordered=False)
        last_id = docs[-1]["commentId"]
        synced_count += len(docs)

    print(f"[{start_comment_id}] completed, synced {synced_count} records")


# ========= 分片边界管理 =========
def load_ranges():
    """加载分片边界（仅第一次计算，后续从文件读取）"""
    if os.path.exists(RANGES_FILE):
        with open(RANGES_FILE, "r") as f:
            ranges = json.load(f)
        print(f"从文件加载分片边界: {RANGES_FILE}")
        return [(int(s), int(e)) for s, e in ranges]

    # 首次运行，需要计算
    src = MongoClient(SRC_URI)[SRC_DB][SRC_COLL]
    first_comment_id = int(src.find_one(sort=[("commentId", 1)])["commentId"])
    last_comment_id = int(src.find_one(sort=[("commentId", -1)])["commentId"])
    print(f"源库commentId范围: {first_comment_id} ~ {last_comment_id}")

    # 动态收集分界点（按数据量均匀分）
    print(f"收集 {PROCESSES} 个分界点...")
    boundaries = []
    total_docs = src.count_documents({})
    docs_per_worker = total_docs / PROCESSES

    cursor = src.find({}, {"commentId": 1}).sort("commentId", 1)
    count = 0
    last_boundary_id = first_comment_id

    for doc in cursor:
        count += 1
        if len(boundaries) >= PROCESSES - 1:
            break
        target_count = docs_per_worker * (len(boundaries) + 1)
        if count >= target_count:
            boundaries.append(int(doc["commentId"]))

    # 构建ranges
    ranges = []
    boundaries_full = [first_comment_id] + boundaries + [last_comment_id]
    for i in range(len(boundaries_full) - 1):
        ranges.append((boundaries_full[i], boundaries_full[i + 1]))

    # 保存到文件
    with open(RANGES_FILE, "w") as f:
        json.dump(ranges, f)
    print(f"分片边界已保存到文件: {RANGES_FILE}")

    return ranges


# ========= Main =========
def main():
    ranges = load_ranges()
    print(f"共 {len(ranges)} 个分片:")
    for i, (s, e) in enumerate(ranges):
        print(f"  Worker {i}: {s} ~ {e}")

    processes = []
    for i, (s, e) in enumerate(ranges):
        p = Process(target=worker, args=(s, e))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()