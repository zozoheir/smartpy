import logging

import logging
import pprint


def getLogger(name, save_to_path=None, format='%(levelname)s | %(name)s | %(asctime)s : %(message)s '):
    logging.basicConfig(format=format, level=logging.INFO)
    logger = logging.getLogger(name)

    if save_to_path:
        fh = logging.FileHandler(save_to_path)
        formatter = logging.Formatter('%(levelname)s | %(name)s | %(asctime)s : %(message)s ')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    def pretty_print(record):
        pretty_message = pprint.pformat(record.msg, indent=4)
        record.msg = pretty_message
        return logging.Formatter.format(record)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter.format = pretty_print
    for handler in logger.handlers:
        handler.setFormatter(formatter)

    return logger