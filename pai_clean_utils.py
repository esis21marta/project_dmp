import os.path as osp
import subprocess

import pai


def clean_pai_path(PAI_RUNS):
    # retrive run info and find exp UUID
    pai.set_config(storage_runs=PAI_RUNS)
    try:
        run_info = pai.load_runs(experiment="no_logging")
    except:
        print("The path does not exist, no logs to clean up.")
        return

    backend = pai.backends.get_backend()
    exp = list(backend.search_experiments(name="no_logging"))[0]
    exp_id = exp.id

    # locate the path to clean
    path = osp.join(PAI_RUNS, "mlruns", exp_id)

    # remove "file://" from local path
    if path.startswith("file://"):
        path = "/" + path[5:].lstrip("/")

    # decide local or hdfs
    if "hdfs://" in path:
        print("Handling HDFS storage {}.".format(path))
        subprocess.call(["hdfs", "dfs", "-rm", "-r", path])
    elif "//" in path:
        print("Only implemented for HDFS and local paths!!")
        return
    else:
        print("Handling local storage {}".format(path))
        subprocess.call(["rm", "-r", path])
    print("Clean up succeed!")
