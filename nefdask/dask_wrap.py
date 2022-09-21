import functools
import logging
import socket
import sys
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler

import treefiles as tf
from dask import distributed
from dask_jobqueue import OARCluster
from distributed import Client, LocalCluster

HOST = socket.gethostname()

# logging.getLogger("distributed.scheduler").setLevel(logging.)
# logging.getLogger("distributed.worker").setLevel(logging.)
# logging.getLogger("distributed.http.proxy").setLevel(logging.)
# logging.getLogger("bokeh").setLevel(logging.)
# logging.getLogger("distributed.nanny").setLevel(logging.)
# logging.getLogger("distributed.core").setLevel(logging.)


@dataclass
class Options:
    cores: int = 4
    walltime: str = tf.walltime(hours=1)
    queue: str = (tf.Queue.BESTEFFORT,)
    job_name: str = "Jo"
    local_n_workers: int = 2
    local_threads_per_worker: int = 2

    def update(self, d):
        for k, v in d.items():
            if hasattr(self, k):
                setattr(self, k, v)


def get_client(opt: Options):
    if HOST.startswith("nef"):
        h, m, s = map(float, opt.walltime.split(":"))
        tsecs = (h * 60 + m) * 60 + s
        mar = min(60, 0.15 * tsecs)
        lifetime = tsecs - mar

        cluster = OARCluster(
            cores=opt.cores,
            memory=0,
            job_name=opt.job_name,
            queue=opt.queue,
            walltime=opt.walltime,
            worker_extra_args=["--lifetime", f"{lifetime}s"],
            job_script_prologue=[],
        )
        # cluster.adapt(...)
    else:
        cluster = LocalCluster(
            processes=True,
            n_workers=opt.local_n_workers,
            threads_per_worker=opt.local_threads_per_worker,
        )

    client = Client(cluster)
    log.info(client)

    return client


def get_worker_logger(fname: str, worker=None, level=logging.INFO):
    if fname.endswith(".py"):
        o = tf.f(fname, "logs", dump=True)
        idx = "" if worker is None else f"_{worker.id}"
        fname = o / tf.basename(fname)[:-3] + f"{idx}.log"

    print(f"Logging task to {fname!r}")

    log_ = logging.getLogger()
    log_.setLevel(level)

    x = RotatingFileHandler(fname, maxBytes=int(1e6), backupCount=5)  # 5*5 Mo max
    x.setFormatter(tf.logs.CSVFormatter())

    # redirect std to file handler
    sys.stdout = sys.stderr = x.stream

    log_.handlers = [x]
    return log_


def dask_main(func):
    @functools.wraps(func)
    def wrapper_dask_main(*args, **kwargs):
        tf.f(func.__globals__["__file__"], "logs").dump(clean=True)
        return func(*args, **kwargs)

    return wrapper_dask_main


def dask_task(func):
    @functools.wraps(func)
    def wrapper_init_logs(*args, **kwargs):
        func.__globals__["log"] = get_worker_logger(
            func.__globals__["__file__"], distributed.get_worker()
        )
        return func(*args, **kwargs)

    return wrapper_init_logs


log = logging.getLogger(__name__)
