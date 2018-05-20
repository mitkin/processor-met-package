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
import ftplib
import netCDF4 as nc
import xarray as xr


def get_topaz_forecast(output_dir):
    baseurl = "http://thredds.met.no/thredds/dodsC/topaz/dataset-topaz4-arc-unmasked-be"
    varnames = [
            # "fice",
            # "hice",
            # "fy_age",
            # "fy_frac",
            # "hsnow",
            "uice",
            "vice",
            "hice",
            "fice",
            ""
            ]
    tdim = 42
    xdim = 608
    ydim = 880
    xmin = 280
    xmax = 420
    ymin = 240
    ymax = 460

    varlist = "[0:1:3][240:1:460][280:1:420],".format(tdim, ydim, xdim).join(varnames)
    varlist += "time[0:1:{}]".format(tdim)
    varlist += ",latitude[240:1:460][280:1:420]"
    varlist += ",longitude[240:1:460][280:1:420]"

    thredds_request = baseurl + "?" + varlist
    xdst = xr.open_dataset(thredds_request)
    comp = dict(zlib=True, complevel=9)
    encoding = {var: comp for var in xdst.variables}

    timestamp = str(xdst.time[0].data).split('T')[0]

    outname = os.path.join(output_dir, 'topaz_{}.nc'.format(timestamp))
    xdst.to_netcdf(outname, engine='netcdf4', encoding=encoding)

def get_weather_forecast(output_dir):
    forecast_url = "http://thredds.met.no/thredds/ncss/aromearcticlatest/arome_arctic_extracted_2_5km_latest.nc?var=air_temperature_2m&var=wind_direction&var=wind_speed&disableLLSubset=on&disableProjSubset=on&horizStride=6&time_start=2018-05-20T06%3A00%3A00Z&time_end=2018-05-25T00%3A00%3A00Z&timeStride=6&vertCoord=&addLatLon=true"
    response = request.urlopen(forecast_url)
    fname = os.path.basename("weather-forecast.nc")
    with open(os.path.join(output_dir, fname), mode='wb') as f:
                f.write(response.read())

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

def create_logger(filepath, loglevel=logging.DEBUG):
    global logger

    logger = logging.getLogger('mapmaker')
    logger.setLevel(loglevel)
    formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

    file_handler = logging.FileHandler(filepath)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

def main():

    p = argparse.ArgumentParser()
    p.add_argument("--log-file", default=None)
    p.add_argument("--output-file", default=None)
    p.add_argument("--request-file", default=None)
    p.add_argument("--download-dir", default=None)
    p.add_argument("-k", "--keep-temporary", action="store_true")
    p.add_argument("--download-only", action="store_true")

    args = p.parse_args()

    create_logger(args.log_file)

    output_dir = args.output_file + '.d'
    try:
        os.mkdir(output_dir)
    except:
        logger.debug("Directory {} already exists".format(output_dir))

    # Obtain data
    get_ice_charts(output_dir)
    get_weather_forecast(output_dir)
    get_topaz_forecast(output_dir)

    shutil.make_archive(os.path.splitext(args.output_file)[0], 'zip', output_dir)

    if not args.keep_temporary:
         shutil.rmtree(output_dir)

if __name__ == "__main__":
    main()
