import argparse
import json
import os
import socket
import subprocess
import sys

import pandas as pd


def dirpath(path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), path))


def filename(path):
    return os.path.splitext(os.path.basename(path))[0]


def get_workdir(jobid, input_file, output):
    return dirpath("runs/{}-{}-{}".format(jobid, filename(input_file), filename(output)))


DASK_PORT = 8786


def run_computation(index, input_file, options):
    from benchmark import compute
    from dask_cluster import start_cluster

    input = options["inputs"][int(index)]
    output = options["outputs"][int(index)]

    workdir = get_workdir(os.environ["PBS_JOBID"], input_file, output)

    if not os.path.exists(workdir):
        os.makedirs(workdir)
    with open(os.path.join(workdir, os.path.basename(input_file)), "w") as dst:
        options["index"] = index
        json.dump(options, dst, indent=4)

    dask_cluster = None
    if options.get("dask"):
        start_cluster(port=DASK_PORT, path=os.getcwd())
        dask_cluster = "{}:{}".format(socket.gethostname(), DASK_PORT)

    with open(os.path.join(workdir, "output"), "w") as out:
        with open(os.path.join(workdir, "error"), "w") as err:
            sys.stdout = out
            sys.stderr = err
            compute(graphset=input,
                    resultfile=output,
                    scheduler=options.get("scheduler", "all"),
                    cluster=options.get("cluster", "all"),
                    bandwidth=options.get("bandwidth", "all"),
                    netmodel=options.get("netmodel", "all"),
                    imode=options.get("imode", "all"),
                    sched_timing=options.get("sched-timing", "all"),
                    repeat=int(options.get("repeat", 1)),
                    timeout=options.get("timeout"),
                    dask_cluster=dask_cluster,
                    skip_completed=True)


def run_pbs(input_file, options):
    from benchmark import load_instances, skip_completed_instances

    args = ["qsub", "-q", "qexp", "-l"]
    nodes = "select={}:ncpus=24"
    if options.get("dask"):
        nodes = nodes.format(8)
    else:
        nodes = nodes.format(1)
    args.append(nodes)

    print("Starting jobs from file {}".format(input_file))
    for i, input in enumerate(options["inputs"]):
        arguments = args[:]

        input = options["inputs"][i]
        output = options["outputs"][i]

        repeat = int(options.get("repeat"))
        instances = load_instances(input, None,
                                   scheduler=options.get("scheduler", "all"),
                                   bandwidth=options.get("bandwidth", "all"),
                                   cluster=options.get("cluster", "all"),
                                   netmodel=options.get("netmodel", "all"),
                                   imode=options.get("imode", "all"),
                                   sched_timing=options.get("sched-timing", "all"),
                                   repeat=repeat)[0]
        if os.path.isfile(output):
            oldframe = pd.read_pickle(output)
            instances = skip_completed_instances(instances, oldframe, repeat)
            if not instances:
                print("All instances were completed for {}".format(input))
                continue

        arguments += ["-N", "estee-{}-{}".format(filename(input_file), filename(output))]
        arguments += ["-v", "ESTEE_INPUT={},ESTEE_INDEX={}".format(
            os.path.abspath(input_file), i)]
        arguments += ["-k", "n"]  # do not keep output

        arguments.append(dirpath("qsub.sh"))

        print("Starting job {}-{}".format(filename(input_file), filename(output)))
        subprocess.run(arguments)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["submit", "compute"])
    parser.add_argument("input_files", nargs="+")
    parser.add_argument("--graph-index", default=0)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.command == "compute":
        assert len(args.input_files) == 1
        file = args.input_files[0]
        with open(file) as f:
            options = json.load(f)
        run_computation(args.graph_index, file, options)
    else:
        for input_file in args.input_files:
            with open(input_file) as f:
                options = json.load(f)
            run_pbs(input_file, options)
