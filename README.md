gdx_grabber
===========

Auto downloader/extractor for  vSPD GDX zip files from http://reports.ea.govt.nz/vSPDDD.aspx

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
