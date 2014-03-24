'''
gdx_grab - automatic downloading of vSPD GDX files

Copyright (C) 2014 Electricty Authority, New Zealand.

This is the gdx_grab class.  Used to connect and download,
and save daily vSPD market data in New Zealand.

We can use this on ad adhoc basis to download and unpack archive files:
For example:
    python gdx_grab.py --year=2013 --archive
        - this downloads the 2013 zip file, if not already downloaded, then
          extracts the individual gdx files to the extraction dir
    python gdx_grab.py --year=2008 --archive --override
        - this downloads and overwrites any zip files, then extracts to
          extraction dir
    Or, used daily with a crontab:
    15 7 * * * /usr/bin/python /home/gdxgrab/gdxgrab.py >> log.tx 2>&1

D J Hume, 24/3/2014
'''

from datetime import datetime
import logging
import logging.handlers
import argparse
import zipfile
import urllib2
import os
from bs4 import BeautifulSoup

#Setup command line option and argument parsing
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--gdx_host', action="store", dest='gdx_host',
                    default='http://www.reports.ea.govt.nz/vSPD/')
parser.add_argument('--gdx_path', action='store', dest='gdx_path',
                    default='/home/dave/vSPD/gdx_grab/')
parser.add_argument('--year', action='store', dest='year', default=datetime.now().year)
parser.add_argument('--archive', action='store_true', dest='archive', default=False)
parser.add_argument('--override', action='store_true', dest='override', default=False)

IPy_notebook = False

if not IPy_notebook:
    cl = parser.parse_args()
if IPy_notebook:
    class cl():
        """Manual setting of cmd_line arguments in iPython notebook"""
        def __init__(self, gdx_host, gdx_path, year, archive, override):
            self.gdx_host = gdx_host
            self.gdx_path = gdx_path
            self.year = year
            self.archive = archive
            self.override = override
    cl = cl('http://www.reports.ea.govt.nz/vSPD/',
            '/home/humed/python/pivot/pivot/gdx_grab/', 2014, False, False)

#Setup logging

formatter = logging.Formatter('|%(asctime)-6s|%(message)s|',
                              '%Y-%m-%d %H:%M:%S')
consoleLogger = logging.StreamHandler()
consoleLogger.setLevel(logging.INFO)
consoleLogger.setFormatter(formatter)
logging.getLogger('').addHandler(consoleLogger)
fileLogger = logging.handlers.RotatingFileHandler(filename='gdx_grab.log',
                                                  maxBytes=1024 * 1024, backupCount=9)
fileLogger.setLevel(logging.ERROR)
fileLogger.setFormatter(formatter)
logging.getLogger('').addHandler(fileLogger)
logger = logging.getLogger('gdx grab ')
logger.setLevel(logging.INFO)


class gdx_grab():
    """This class acts as an automatic downloader for NZ electricity market
       vSPD GDX files.  There are two modes of operation:
            (a) archival mode --archive;
                    - downloads all Final price GDX files from the start of
                      year to current;
                    - loops through years, download zips (use --overide to overwrite)
                      decompresses/extracts GDX files from zip;
                    - then downloads individual GDX files from current month.
            (b) daily mode (default);
                    - use with a daily crontab entry for most recent GDX file.
                    - downloads and over-wrights last month of GDX files.
    """

    def __init__(self, gdx_host, gdx_path, year, archive, override):
        self.gdx_host = gdx_host
        self.gdx_path = gdx_path
        self.year = int(year)
        self.archive = archive
        self.override = override
        self.gdx_zipfile = str(year) + "_vSPD_GDX_Files.zip"
        self.gdx_zip = gdx_host + "Archives/" + self.gdx_zipfile
        self.gdx_ext = gdx_path + "extracted/"
        self.test = True
        self.ml = 88

    def gdx_arch(self, zfile):
        """Given year, download GDX archive for that year, extract GDX files"""
        if ((not os.path.isfile(zfile)) or self.override):
            msg = "Downloading GDX archive for %s" % self.year
            logger.info(msg.center(self.ml, ' '))
            r = urllib2.urlopen(self.gdx_zip)
            gdxzip = r.read()
            msg = "Saving to %s" % zfile
            logger.info(msg.center(self.ml, ' '))
            output = open(zfile, 'wb')
            output.write(gdxzip)
            output.close()
        else:
            msg = "Using existing archive zipfile %s" % zfile
            logger.info(msg.center(self.ml, ' '))

        zfobj = zipfile.ZipFile(zfile)
        print zfile
        for name in zfobj.namelist():
            uncomp = zfobj.read(name)
            otptname = self.gdx_ext + name
            if self.test:
                msg = "Extract to %s" % otptname
                logger.info(msg.center(self.ml, ' '))
            output = open(otptname, 'wb')
            output.write(uncomp)
            output.close()

    def gdx_last_month(self):
        """Download individual GDX files over the most recent month"""
        r = urllib2.urlopen(self.gdx_host)
        s = BeautifulSoup(r)
        msg = "Updating final price GDX files over last month"
        logger.info(msg.center(self.ml, ' '))
        for a in s.find_all('a', href=True):
            if 'FP_' in a['href']:
                if "_I" not in a['href']:  # ignore interim pricing
                    name = a['href'].split('/')[2]
                    r = urllib2.urlopen(self.gdx_host + name)
                    gdx = r.read()
                    if self.test:
                        msg = "Saving to %s" % self.gdx_ext + name
                        logger.info(msg.center(self.ml, ' '))
                    output = open(self.gdx_ext + name, 'wb')
                    output.write(gdx)
                    output.close()

    def dl_archive(self):
        """If archive mode True; loop through all years to current year, then
            download last months worth of data"""
        if not os.path.isdir(self.gdx_ext):
            msg = "Create extraction directory at: %s" % self.gdx_ext
            logger.info(msg.center(self.ml, ' '))
            os.mkdir(self.gdx_ext)

        while int(self.year) <= int(datetime.now().year):
            msg = "Grab and extract gdx files for %s" % str(self.year)
            logger.info(msg.center(self.ml, ' '))
            self.gdx_arch(self.gdx_zipfile)
            self.year += 1
            self.gdx_zipfile = str(self.year) + "_vSPD_GDX_Files.zip"
            self.gdx_zip = self.gdx_host + "Archives/" + self.gdx_zipfile

        self.gdx_last_month()

    def dl_daily(self):
        """If archive mode False; overright the last months worth of data"""
        self.gdx_last_month()

if __name__ == '__main__':
    gx = gdx_grab(cl.gdx_host, cl.gdx_path, cl.year, cl.archive, cl.override)  # run instance
    if cl.archive:
        msg = "Archival mode - download zip files then current month files"
        logger.info(msg.center(88, ' '))
        gx.dl_archive()
    if not cl.archive:
        msg = "Daily download mode - download current GDX file"
        logger.info(msg.center(88, ' '))
        gx.dl_daily()
