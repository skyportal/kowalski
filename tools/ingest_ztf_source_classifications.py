import fire
import h5py
import multiprocessing as mp
import os
import pandas as pd
import pathlib
import pymongo
import traceback
from tqdm import tqdm


import istarmap  # noqa: F401
from utils import (
    init_db_sync,
    load_config,
    log,
    Mongo,
)


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]
# init db if necessary
init_db_sync(config=config)


def process_file(_file, _collections):

    # connect to MongoDB:
    mongo = Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        replica_set=config["database"]["replica_set"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        db=config["database"]["db"],
        verbose=0,
    )

    try:
        with h5py.File(_file, "r") as f:
            # print(f.keys())
            predictions = f["preds"][...]

        # parse basic model metadata
        architecture = str(_file).split("/")[-3]
        cls = str(_file).split("/")[-2]
        if architecture == "xgb":
            cls = cls.split(".")[1]
        elif architecture == "dnn":
            cls = cls.split("-")[0]

        column_names = ["_id", f"{cls}_{architecture}"]

        df = pd.DataFrame(predictions, columns=column_names)
        df["_id"] = df["_id"].apply(lambda x: int(x))

        docs = df.to_dict(orient="records")

        requests = []

        for doc in docs:

            _id = doc["_id"]
            doc.pop("_id", None)
            requests += [
                pymongo.UpdateOne(
                    {"_id": _id},
                    {"$set": doc},
                    upsert=True,
                )
            ]

        mongo.db[_collections["classifications"]].bulk_write(requests)

    except Exception as e:
        traceback.print_exc()
        print(e)

    # disconnect from db:
    try:
        mongo.client.close()
    finally:
        pass


def run(
    path: str = "./",
    tag: str = "DR5",
    num_processes: int = mp.cpu_count(),
):
    """Pre-process and ingest ZTF source classifications from SCoPe

    :param path: path to root folder of the directories containing
                 hdf5 files containing ZTF source classifications
    :param tag: from collection name: ZTF_source_classifications_<YYYYMMDD>
    :param num_processes:
    :return:
    """
    # connect to MongoDB:
    log("Connecting to DB")
    mongo = Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        replica_set=config["database"]["replica_set"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        db=config["database"]["db"],
        verbose=0,
    )
    log("Successfully connected")

    collections = {"classifications": f"ZTF_source_classifications_{tag}"}

    if config["database"]["build_indexes"]:
        log("Checking indexes")
        try:
            mongo.db[collections["classifications"]].create_index(
                [
                    ("vnv_xgb", pymongo.DESCENDING),
                ],
                background=True,
            )
            mongo.db[collections["classifications"]].create_index(
                [
                    ("vnv_dnn", pymongo.DESCENDING),
                ],
                background=True,
            )
        except Exception as e:
            log(e)

    root = pathlib.Path(path)
    files = []
    for architecture in ("xgb", "dnn"):
        files.extend((root / architecture).glob("*/*.h5"))

    input_list = [(f, collections) for f in sorted(files) if os.stat(f).st_size != 0]

    log(f"# files to process: {len(input_list)}")
    # process_file(*input_list[0])

    with mp.Pool(processes=num_processes) as p:
        for _ in tqdm(p.istarmap(process_file, input_list), total=len(input_list)):
            pass

    log("All done")


if __name__ == "__main__":
    fire.Fire(run)
