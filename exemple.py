import logging

import treefiles as tf

import packageName as pk


def main():
    bs = pk.Class()

    log.info(bs)
    log.debug(bs.__class__.__mro__)


log = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    log = tf.get_logger()

    main()
