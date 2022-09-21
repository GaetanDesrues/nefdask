import logging
import sys

import dask
import treefiles as tf


class SHdlr(logging.StreamHandler):
    def __init__(self):
        super().__init__(sys.stdout)
        self.setFormatter(tf.logs.SimpleFormatter())


def init_dask():  # *infos):
    """
    Init dask loggers

    To modify a logger:
        logging.getLogger("distributed.scheduler").setLevel(logging.INFO)
    """
    defs = {"level": logging.ERROR, "handlers": ["console"]}
    loggers = {
        "distributed.scheduler",
        "distributed.worker",
        "distributed.nanny",
        "distributed.core",
        "distributed.http.proxy",
        "bokeh",
    }
    logging_config = {
        "version": 1,
        "handlers": {"console": {"class": "nefdask.init_dask.SHdlr"}},
        "loggers": {k: dict(defs) for k in loggers},
    }
    # for x in infos:
    #     if x in logging_config["loggers"]:
    #         logging_config["loggers"][x]["level"] = logging.INFO

    dask.config.config["logging"] = logging_config
