import json
import os
import urllib

import fsspec
import pai

PAI_RUNS = "hdfs:///data/landing/dmp_remote/model_factory/yuan/inlife_modelling/pai"
EXP_NAME = "model_factory"


def main():
    # set PAI_RUNS & initialise cache
    pai.set_config(storage_runs=PAI_RUNS)
    _ = pai.load_runs(experiment=EXP_NAME)

    # get backend to search experiment id
    backend = pai.backends.get_backend()
    experiments = list(backend.search_experiments(name=EXP_NAME))
    EXP_IDS = [e.id for e in experiments]

    # initialise filesystem interface
    parsed_url = urllib.parse.urlparse(PAI_RUNS)
    protocol = parsed_url.scheme or "file"
    fs = fsspec.filesystem(protocol=protocol)

    # delete
    for EID in EXP_IDS:
        print("Scanning experiment id {}".format(EID))
        # interate through each run folder
        run_folders = [
            rf["name"]
            for rf in fs.listdir(os.path.join(PAI_RUNS, "mlruns", EID))
            if not rf["name"].endswith("experiment.json")
        ]
        for run in run_folders:
            try:
                run_json = os.path.join(run, "run.json")
                with fs.open(run_json) as json_file:
                    f = json_file.read().decode("utf-8")
                    run_info = json.loads(f)
                    status = run_info["info"]["lifecycle_stage"]
                    if status == "deleted":
                        fs.rm(run, recursive=True)
                        print("Deleted run {} under experiment {}".format(run, EID))
                json_file.close()
            except:
                pass


if __name__ == "__main__":
    main()
