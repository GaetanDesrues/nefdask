import logging

import treefiles as tf

from nefdask.init_dask import init_dask

init_dask()  # init dask loggers before importing dask modules

from nefdask.dask_wrap import Options, get_client, dask_main, dask_task


@dask_main
def main(**kw):
    """
    Main function: initialise dask client and submit tasks to workers
    """
    # Optionally modify dask loggers
    # logging.getLogger("distributed.scheduler").setLevel(logging.INFO)

    opt = Options(
        # local
        local_n_workers=4,
        local_threads_per_worker=2,
        # nef
        cores=1,
        walltime=tf.walltime(minutes=1),
    )
    opt.update(kw)
    client = get_client(opt)

    futures = client.map(task, tuple(range(8)))
    res = client.gather(futures)

    print("result:", res)
    log.info(f"Finished client")

    client.close()


@dask_task
def task(i):
    """
    This task will be executed on a recruited worker
    """

    log.debug(f"debug -> I'm task {i}")
    log.info(f"info -> I'm task {i}")
    log.warning(f"warning -> I'm task {i}")
    log.error(f"error -> I'm task {i}")
    print("worker print:", f"I'm task {i}")

    return f"task-{i}"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log = tf.get_logger()

    main(local_n_workers=2, local_threads_per_worker=4)
    # play with these two settings and search for log files (./logs/)
    # The number of files (local_n_workers), the number of tasks in
    # each file (8 / local_n_workers*local_threads_per_worker) and their order varies.

    # local_n_workers=1, local_threads_per_worker=1
    # local_n_workers=1, local_threads_per_worker=8
    # local_n_workers=8, local_threads_per_worker=1
    # local_n_workers=4, local_threads_per_worker=2
    # ...

    # Number of parallel tasks at each time = local_n_workers*local_threads_per_worker
