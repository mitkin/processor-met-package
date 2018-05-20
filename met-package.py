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
from urllib import request
from zipfile import ZipFile
import io
from io import StringIO
import requests
import tempfile
import ftplib


def get_ice_charts(output_dir, add_date=True):
    """
    Figure out the date the chart was posted and obtain it
    Function will download files and put it to the output_dir
    """
    hostname = "ftp.met.no"
    dir_path = "projects/icecharts"
    basename = "chart_ice"
    exts = ["shp", "dbf", "shx", "prj"]

    with ftplib.FTP(hostname) as met_ftp:
        met_ftp = ftplib.FTP(hostname)
        met_ftp.login()
        met_ftp.cwd(dir_path)
        date_str = met_ftp.sendcmd('MDTM {}.shp'.format(basename))
        timestamp = datetime.datetime.strptime(date_str.split()[1], '%Y%m%d%H%M%S')
        time_str = datetime.datetime.strftime(timestamp, "%Y%m%d")

        logger.info("Ice chart upload date: {}".format(time_str))

        for e in exts:
            response = request.urlopen("ftp://{}/{}/{}.{}".format(
                hostname,
                dir_path,
                basename,
                e))
            with open(os.path.join(
                        output_dir,
                        basename + "_{}".format(time_str) + "." + e), 'wb') as f:
                f.write(response.read())

def main():

    p = argparse.ArgumentParser()
    p.add_argument("--log-file", default=None)
    p.add_argument("--output-file", default=None)
    p.add_argument("--request-file", default=None)
    p.add_argument("--download-dir", default=None)
    p.add_argument("-k", "--keep-temporary", action="store_true")
    p.add_argument("--download-only", action="store_true")

    args = p.parse_args()

    global logger

    logger = logging.getLogger('mapmaker')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

    file_handler = logging.FileHandler(args.log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    output_dir = args.output_file + '.d'
    try:
        os.mkdir(output_dir)
    except:
        logger.debug("Directory {} already exists".format(output_dir))

    get_ice_charts(output_dir)

    shutil.make_archive(os.path.splitext(args.output_file)[0], 'zip', output_dir)


    if not args.keep_temporary:
         shutil.rmtree(output_dir)

if __name__ == "__main__":
    main()
