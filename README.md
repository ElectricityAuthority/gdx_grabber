gdx_grabber
===========

Auto downloader/extractor for  vSPD GDX zip files from http://reports.ea.govt.nz/vSPDDD.aspx

Copyright (C) 2014 Electricity Authority, New Zealand.
See, https://github.com/ElectricityAuthority/LICENSE/blob/master/LICENSE.md

Used to connect, download, extract and save daily vSPD Electricity market data.

Used either on:
  1. an adhoc basis, for occasional download and unpacking of archive files, or;
  2. daily via crontab/scheduler of your choice:

For example:

    python gdx_grab.py --year=2013 --archive

this downloads the 2013 zip file, if not already downloaded, then extracts the individual gdx files to the extraction dir

    python gdx_grab.py --year=2008 --archive --override
this downloads and overwrites any zip files, then extracts to extraction dir. 

Or, can be used daily with a crontab:

    15 7 * * * /usr/bin/python /home/gdxgrab/gdxgrab.py >> log.tx 2>&1
this downloads and overwrites the last month of GDX files, as per http://reports.ea.govt.nz/vSPDDD.aspx

D J Hume, 24/3/2014
