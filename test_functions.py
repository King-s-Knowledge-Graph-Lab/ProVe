import pickle
import os
import time

from tqdm import tqdm
import numpy as np

from functions import get_item, GetItem


if __name__ == "__main__":

    max_qid = 1000000
    iterable = range(1, 1000000)
    progress = {x: False for x in iterable}
    progress["last"] = 0
    progress["new"] = []
    progress["old"] = []

    if os.path.exists("progress.pickle"):
        with open("progress.pickle", "rb") as handle:
            progress = pickle.load(handle)

        iterable = range(progress["last"] + 1, max_qid)

    try:
        pbar = tqdm(iterable)
        for i in pbar:
            if progress["last"] > 0:
                desc = sum(list(progress.values())[:progress["last"]]) / progress["last"]
                pbar.set_description(f"Equal: {str(round(desc * 100, 4))}%")

                new_post = f"New AVG time: {round(np.mean(progress['new']), 4)}s"
                old_post = f"Old AVG time: {round(np.mean(progress['old']), 4)}s"
                pbar.set_postfix_str(f"{new_post} - {old_post}")
            qid = f"Q{str(i)}"

            start_old = time.perf_counter()
            old = GetItem(qid)
            end_old = time.perf_counter()

            start_new = time.perf_counter()
            new = get_item(qid)
            end_new = time.perf_counter()

            progress[i] = new == old
            progress["last"] = i
            progress["new"].append(end_new - start_new)
            progress["old"].append(end_old - start_old)
    except Exception as e:
        print(e)
    finally:
        with open("progress.pickle", "wb") as handle:
            pickle.dump(progress, handle, protocol=pickle.HIGHEST_PROTOCOL)
