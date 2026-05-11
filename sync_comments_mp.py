from pymongo import MongoClient, ReplaceOne
from multiprocessing import Process, cpu_count
import logging
import math

from bson import ObjectId
# # ========= 日志 =========
# logging.basicConfig(
#     filename="sync_comments_mp.log",
#     level=logging.INFO,
#     format="%(asctime)s %(levelname)s %(message)s"
# )

# ========= MongoDB =========
SRC_URI = "mongodb://localhost:27017"
DST_URI = "mongodb://192.168.3.8:27017"

BATCH_SIZE = 100000
# PROCESSES = min(8, cpu_count())
PROCESSES = 24



def get_id_range():
    src = MongoClient(SRC_URI)["netease"]["comments"]
    first = src.find().sort("_id", 1).limit(1)[0]["_id"]
    last = src.find().sort("_id", -1).limit(1)[0]["_id"]
    return first, last

def worker(start_id, end_id):
    src = MongoClient(SRC_URI, maxPoolSize=PROCESSES)["netease"]["comments"]
    dst = MongoClient(DST_URI, maxPoolSize=PROCESSES)["netease"]["comments"]

    last_doc = dst.find({
        "_id": {"$gte": start_id, "$lte": end_id}
    }).sort("_id", -1).limit(1)

    if last_doc.count() > 0:
        last_id = last_doc[0]["_id"]
        print(f"[{start_id}] resume from {last_id}")
    else:
        last_id = start_id
        print(f"[{start_id}] start from beginning")

    while True:
        query = {
            "_id": {
                "$gt": last_id,
                "$lt": end_id
            }
        }
        cursor = src.find(query).sort("_id", 1).limit(BATCH_SIZE)
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
        last_id = docs[-1]["_id"]
        print(f"[{start_id}] synced up to {last_id}")


def main():
    # logging.info("Start multi-process sync")
    start_id, end_id = get_id_range()

    start_int = int(str(start_id), 16)
    end_int = int(str(end_id), 16)

    step = math.ceil((end_int - start_int) / PROCESSES)



    processes = []

    for i in range(PROCESSES):
        s = ObjectId(hex(start_int + step * i)[2:])
        e = ObjectId(hex(min(
            start_int + step * (i + 1),
            end_int
        ))[2:])

        p = Process(target=worker, args=(s, e))
        p.start()
        processes.append(p)


    for p in processes:
        p.join()






    # logging.info("✅ All workers completed")


if __name__ == "__main__":
    main()