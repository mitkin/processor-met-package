#!/home/mikhail/.virtualenvs/def36/bin/python

import argparse
import datetime
import glob
import os
import subprocess
from zipfile import ZipFile
import multiprocessing
from functools import partial
import logging
import shutil
import json
import sys
from sentinelsat import SentinelAPI, geojson_to_wkt
import rasterio
from pyproj import Proj
import affine
import numpy




def main():

    p = argparse.ArgumentParser()
    p.add_argument("--log-file", default=None)
    p.add_argument("--output-file", default=None)
    p.add_argument("--request-file", default=None)
    p.add_argument("--download-dir", default=None)
    p.add_argument("-k", "--keep-temporary", action="store_true")
    p.add_argument("--download-only", action="store_true")

    args = p.parse_args()

    logger = logging.getLogger('mapmaker')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

    file_handler = logging.FileHandler(args.log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if args.request_file is not None:
        global request
        request = json.loads(open(args.request_file).read())
    else:
        logger.info("Request file missing, aborting")
        raise Exception("Can't proceed without request json file")

if __name__ == "__main__":
    main()
