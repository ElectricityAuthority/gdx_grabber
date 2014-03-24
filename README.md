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
