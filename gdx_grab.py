'''
gdx_grabber
===========

Auto downloader/extractor for vSPD GDX zip files from
http://emi.ea.govt.nz/Datasets/Wholesale/Final_pricing/GDX

Copyright (C) 2014 Electricity Authority, New Zealand.
https://github.com/ElectricityAuthority/LICENSE/blob/master/LICENSE.md

Used to connect, download, extract, save vSPD GDX files and can also be
used to produce the vSPD FileNameList.inc file, given start and end times
of files in the extraction dir.

gdx_grabber has been developed to help market performance automate vSPD
experimental runs; e.g., various market power measures such as pivotalness
and/or Inverse Residual Demand Elasticity.

For help, run: python gdx_grab.py --help

Two mode, mutually exclusive, operation:

    -d, --download      download mode
    -f, --filelist      filelist mode

Download mode -d, --download
============================

Used either on:
  1. an adhoc basis, for occasional download and unpack of archives, or;
  2. daily via crontab/scheduler of your choice:

For example:

    python gdx_grab.py -d --year=2013 --archive

downloads then extracts all gdx files starting 1 January, 2013 to the
extraction dir.

    python gdx_grab.py --year=2008 --archive --override

downloads then extracts all gdx files since 1 January 2008 to the
extraction dir.  Specifing --override, overwrites existing annual zip
files.

Default use is for periodic updates; i.e. daily with crontab/schedular:

    15 7 * * * /usr/bin/python /home/gdxgrab/gdxgrab.py >> log.tx 2>&1

downloads and overwrites the last month of GDX files, as per
http://emi.ea.govt.nz/Datasets/Wholesale/Final_pricing/GDX

Filelist mode -f, --filelist
============================

This mode is used to automatically generate the vSPD FileNameList.inc file,
prior to a vSPD run.  It is set up to run either on an adhoc basis, or with
a crontab/schedular of your choice.  Output will overright any existing
FileNameList.inc file in the current working directory.  Shell script may
be required to mv FileNameList.inc to the vSPD Programs dir.

Note: GDX files must be present in the extraction directory for this to
succeed.

For example:

    python gdx_grab.py -f -s 2014-02 -e 2014-02

creates FileNameList.inc file with all February 2014, GDX files.

   python gdx_grab.py -f -s 2008-01-01 -e 2008-01-21

creates FileNameList.inc file which lists the GDX filenames between 1 Jan,
2008 and 21 Jan 2008.

D J Hume, 25/3/2014
'''

from datetime import datetime, date
import logging
import logging.handlers
import argparse
import zipfile
import urllib2
import os
from bs4 import BeautifulSoup
#Setup command line option and argument parsing
parser = argparse.ArgumentParser(description='vSPD automatic GDX file downloader, extractor and FileNameList.inc generator')
group = parser.add_mutually_exclusive_group()
group.add_argument('-d', '--download', action='store_true', dest='download',
                   help='download mode')
group.add_argument('-f', '--filelist', action='store_true', dest='filelist',
                   help='filelist mode')
parser.add_argument('--gdx_host', action="store", dest='gdx_host',
                    default = 'http://www.emi.ea.govt.nz/Datasets/Wholesale/Final_pricing/GDX/',
                    help='url pointer (default: http://www.emi.ea.govt.nz/Datasets/Wholesale/Final_pricing/GDX/')
parser.add_argument('--gdx_path', action='store', dest='gdx_path',
                    default=os.getcwd() + '/',
                    help='path for archive zip file downloads and extraction dir (default: current working directory)')
parser.add_argument('--year', action='store', dest='year', default=datetime.now().year,
                    help='year (default = %s)' % datetime.now().year)
parser.add_argument('--archive', action='store_true', dest='archive', default=False,
                    help='archive mode, downloads all GDX files since 1 January for the given --year')
parser.add_argument('--override', action='store_true', dest='override', default=False,
                    help='write over any previously saved archive zip files')
parser.add_argument('-s', '--start', action='store', dest='start', default=date(datetime.now().year - 1, 1, 1),
                    help='start date required for generation of FileNameList.inc (default: %s)' % str(date(datetime.now().year - 1, 1, 1)))
parser.add_argument('-e', '--end', action='store', dest='end', default=date(datetime.now().year - 1, 12, 31),
                    help='end date required for generation of FileNameList.inc (default: %s)' % str(date(datetime.now().year - 1, 12, 31)))


IPy_notebook = False

if not IPy_notebook:
    cl = parser.parse_args()
if IPy_notebook:
    class cl():
        """Manual setting of cmd_line arguments in iPython notebook"""
        def __init__(self, download, filelist, gdx_host, gdx_path, year, archive, override, start, end):
            self.download = download
            self.filelist = filelist
            self.gdx_host = gdx_host
            self.gdx_path = gdx_path
            self.year = year
            self.archive = archive
            self.override = override
            self.start = start
            self.end = end
    cl = cl('http://emi.ea.govt.nz/Datasets/Wholesale/Final_pricing/GDX/',
            '/home/humed/python/pivot/pivot/gdx_grab/', 2014, False, False,
            date(datetime.now().year - 1, 1, 1), date(datetime.now().year - 1, 12, 31))

#Setup logging
log = logging.getLogger('gdx_grab')
log.setLevel(logging.INFO)  # print everything above INFO
formatter = logging.Formatter('|%(asctime)-6s|%(message)s|',
                              '%Y-%m-%d %H:%M:%S')
logstream = logging.StreamHandler()
logstream.setFormatter(formatter)
log.addHandler(logstream)


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

    def __init__(self, download, filelist, gdx_host, gdx_path, year, archive, override, start, end):
        self.download = download
        self.filelist = filelist
        self.gdx_host = gdx_host
        self.gdx_path = gdx_path
        self.year = int(year)
        self.archive = archive
        self.override = override
        self.start = start
        self.end = end
        self.gdx_zipfile = str(year) + "_vSPD_GDX_Files.zip"
        self.gdx_zip = gdx_host + "Archives/" + self.gdx_zipfile
        self.gdx_ext = gdx_path + "extracted/"
        self.test = True
        self.ml = 88

    def gdx_arch(self, zfile):
        """Given year, download GDX archive for that year, extract GDX files"""
        if ((not os.path.isfile(zfile)) or self.override):
            msg = "Downloading GDX archive for %s" % self.year
            log.info(msg.center(self.ml, ' '))
            r = urllib2.urlopen(self.gdx_zip)
            gdxzip = r.read()
            msg = "Saving to %s" % zfile
            log.info(msg.center(self.ml, ' '))
            output = open(zfile, 'wb')
            output.write(gdxzip)
            output.close()
        else:
            msg = "Using existing archive zipfile %s" % zfile
            log.info(msg.center(self.ml, ' '))

        zfobj = zipfile.ZipFile(zfile)
        for name in zfobj.namelist():
            uncomp = zfobj.read(name)
            otptname = self.gdx_ext + name
            if self.test:
                msg = "Extract to %s" % otptname
                log.info(msg.center(self.ml, ' '))
            output = open(otptname, 'wb')
            output.write(uncomp)
            output.close()

    def gdx_last_month(self):
        """Download individual GDX files over the most recent month"""
        r = urllib2.urlopen(self.gdx_host)
        s = BeautifulSoup(r)
        msg = "Updating final price GDX files over last month"
        log.info(msg.center(self.ml, ' '))
        for a in s.find_all('a', href=True):
            if 'FP_' in a['href']:
                if "_I" not in a['href']:  # ignore interim pricing
                    name = a['href'].split('/')[-1]
                    r = urllib2.urlopen(self.gdx_host + name)
                    gdx = r.read()
                    if self.test:
                        msg = "Saving to %s" % self.gdx_ext + name
                        log.info(msg.center(self.ml, ' '))
                    output = open(self.gdx_ext + name, 'wb')
                    output.write(gdx)
                    output.close()

    def extract_dir(self):
        """Create extraction dir if not there"""
        if not os.path.isdir(self.gdx_ext):
            msg = "Create extraction directory at: %s" % self.gdx_ext
            log.info(msg.center(self.ml, ' '))
            os.mkdir(self.gdx_ext)

    def dl_archive(self):
        """If archive mode True; loop through all years to current year, then
            download last months worth of data"""
        while int(self.year) <= int(datetime.now().year):
            msg = "Grab and extract gdx files for %s" % str(self.year)
            log.info(msg.center(self.ml, ' '))
            self.gdx_arch(self.gdx_zipfile)
            self.year += 1
            self.gdx_zipfile = str(self.year) + "_vSPD_GDX_Files.zip"
            self.gdx_zip = self.gdx_host + "Archives/" + self.gdx_zipfile

        self.gdx_last_month()

    def dl_daily(self):
        """If archive mode False; overright the last months worth of data"""
        self.gdx_last_month()

    def filenamelist(self):
        """Create the FileNameList.inc file for vSPD runs based on a start and end date."""
        import pandas as pd
        fnl = pd.DataFrame({'filename': pd.Series(os.listdir(self.gdx_ext))})
        fnl = fnl.ix[fnl.filename.map(lambda x: x[0:3] == 'FP_'), :]
        fnl.filename = fnl.filename.map(lambda x: x.split('.')[0])
        fnl.index = fnl.filename.map(lambda x: datetime(int(x[3:7]), int(x[7:9]), int(x[9:11])))
        fnl = fnl.sort()
        f = open('FileNameList.inc', 'w')
        for fn in fnl.ix[self.start:self.end, :].filename.values:
            f.write(fn + '\n')
        f.close()


if __name__ == '__main__':
    gx = gdx_grab(cl.download, cl.filelist, cl.gdx_host, cl.gdx_path, cl.year, cl.archive, cl.override, cl.start, cl.end)  # run instance
    if cl.download:
        gx.extract_dir()
        if cl.archive:
            msg = "Archival mode - download zip files then current month files"
            log.info(msg.center(88, ' '))
            gx.dl_archive()
        if not cl.archive:
            msg = "Daily download mode - download current GDX file"
            log.info(msg.center(88, ' '))
            gx.dl_daily()
    if cl.filelist:
        gx.filenamelist()
