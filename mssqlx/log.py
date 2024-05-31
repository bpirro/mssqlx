""" Logging functions for ETL processing.

Common library of logging functions for use with Omnicell BI_Analytics ETL.
"""

import logging
import os

def configure_logger(calling_script_name):
    # create custom logger
    # logger = logging.getLogger(__name__)
    logger = logging.getLogger(os.path.basename(calling_script_name))
    logger.setLevel(logging.DEBUG)

    # create logging handlers
    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(calling_script_name[0:-3] + ".log")

    stream_handler.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.INFO)

    # create log formatters
    stream_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    stream_handler.setFormatter(stream_format)
    file_handler.setFormatter(file_format)

    # add handlers to the logger
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger
