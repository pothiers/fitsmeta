#! /usr/bin/env python
"""Produce histograms of FITS header keyword usage."""

# EXAMPLES:
#  python3 ./kwhistos.py ~/data data.db
#  python3 ./kwhistos.py /net/archive/mtn all-mtn.kwhistos.db
#
# sqlite3 -header -column data.db 'select * from kwcount order by count' | tail
# sqlite3 -header -column data.db 'select * from fingerprint order by fpid'
# sqlite3 -header -column data.db 'select * from fpfile order by fpid'
#
# fcnt1=`find ~/data -name "*.fits.fz" -o -name "*.fits" | wc -l`
# fcnt2=`sqlite3 data.db 'select count(filename) from fpfile'`
# echo "$fcnt1 == $fcnt2 ?"  # SHOULD be equal



import sys
import argparse
import logging
import glob
import os
import os.path
import pickle
import sqlite3
import itertools
import warnings
import random
import time
import dbm

import astropy.io.fits as pyfits
from astropy.utils.exceptions import AstropyWarning
from collections import Counter, defaultdict
from pprint import pprint

def tic():
    tic.start = time.perf_counter()

def toc():
    elapsed_seconds = time.perf_counter() - tic.start
    return elapsed_seconds # fractional


def kw_set(fitsfname):
    # keywords that may have duplicates (we don't care about them)
    nukem = set(['COMMENT','HISTORY',''])
    kws = list()
    try:
        hdulist = pyfits.open(fitsfname)
        for hdu in hdulist:
            kws.extend(hdu.header.keys())
        hdulist.close()
    except:
        print('ERROR reading file "{}" with astropy. Ignoring!'
              .format(fitsfname))
        frozenset()
    return frozenset(set(kws)-nukem)

def fits_iter(topdir):
    gfz = os.path.join(topdir,'**','*.fz')
    gfits = os.path.join(topdir,'**','*.fits')
    return(itertools.chain(glob.iglob(gfz, recursive=True),
                           glob.iglob(gfits, recursive=True)))

def file_dblist(topdir, dbmfile='kwhistos.dbm'):
    with dbm.open(dbmfile,'n') as db:
        for fname in fits_iter(topdir):
            db[fname] = 1
        

# speed: 15.919s(real, chimp)/468 local files
# speed: 7.775
def kw_histo(topdir, progfcnt=100):
    """Count usage of fields (max one per file) in all FITS under TOPDIR"""
    cnt = Counter()
    fcnt=0
    hcnt=0
    for fname in fits_iter(topdir):
        fcnt += 1
        if (fcnt % progfcnt) == 0:
            print('# processed {} files'.format(fcnt))
        #print('fname={}'.format(fname))
        #kws,hducnt = kw_set(fname)
        #hcnt += hducnt
        kws = kw_set(fname)
        cnt.update(kws)
    
    #print('Counts of fields in {} files:'.format(fcnt))
    #pprint(cnt)
    print('Percentage of HDUs using field in {} files:'.format(fcnt))
    perc = [(k,v/float(fcnt)) for k,v in cnt.items()]
    pprint(sorted(perc,  key=lambda x: x[1], reverse=True))

# speed: 14.456s(real, chimp)/468 local files
# speed: 5.622
# speed: 5.531
def kw_fingerprints(topdir, progfcnt=100):
    """Get sets of keywords (per file) in all FITS under TOPDIR"""
    cnt = Counter()
    fcnt=0
    fps = dict()
    for fname in fits_iter(topdir):
        fcnt += 1
        if (fcnt % progfcnt) == 0:
            print('# processed {} files'.format(fcnt))
        kws = kw_set(fname)
        cnt[kws] += 1
    print('KW Finger Print usage in {} files:'.format(fcnt))
    perc = [(k,v/float(fcnt)) for k,v in cnt.items()]
    pprint(sorted(perc,  key=lambda x: x[1], reverse=True))

make_db_sql = """    
CREATE TABLE kwcount(
        kw TEXT PRIMARY KEY,
        count INTEGER
        );
CREATE TABLE fpfile(
        filename TEXT PRIMARY KEY,
        fpid TEXT
        );
CREATE TABLE fingerprint(
        kw TEXT,
        fpid TEXT,
        PRIMARY KEY (kw, fpid)
        );
"""
    
def kw_use(topdir, db, progfcnt=10, kwfile=None, fpfile=None):
    """Collect several counts at once"""
    if os.path.exists(db):
        os.remove(db)
    con = sqlite3.connect(db)
    badfits = set()
    with con:
        con.executescript(make_db_sql)
        # FP:: finger print (SET of keywords in one file)
        kwcnt = Counter() # kwcnt[kw]=count (accume over all files)
        fpcnt = Counter() # fpcnt[fpid]=count 
        fpids = dict()    # fpids[kws] = fpid
        #!fnames = defaultdict(list)   # fpids[kws] = [fname,...]

        fcnt=0
        fps = dict()
        #all = dict()
        warnings.simplefilter('ignore', category=AstropyWarning)
        print('COLLECT filenames')
        allfits = list(fits_iter(topdir))
        print('SHUFFLE {} filenames'.format(len(allfits)))
        random.shuffle(allfits)
        print('PROCESS filenames')
        tic()
        for fname in allfits:
            kws = kw_set(fname)
            if len(kws) == 0:
                badfits.add(fname)
                continue
            fcnt += 1
            #!fnames[kws].append(fname)
            fpid = fpids.setdefault(kws, 'fp-{:07d}'.format(fcnt))
            # add counts for this file
            kwcnt.update(kws)  # count keyword use over ALL files
            fpcnt[fpid] += 1   # count fingerprint use of ALL files

            # Update DB with keyword occurances for this file
            con.executemany('INSERT OR REPLACE INTO'
                            ' kwcount (kw, count) VALUES (?,?)',
                            [(kw,kwcnt[kw]) for kw in kws])
            con.executemany('INSERT OR REPLACE INTO'
                            ' fingerprint (kw, fpid) VALUES (?,?)',
                            [(kw, fpid) for kw in kws])
            con.execute('INSERT OR REPLACE INTO'
                        ' fpfile (filename, fpid) VALUES (?,?)',
                        (fname, fpid))
            con.commit()
            
            if (fcnt % progfcnt) == 0:
                print('# processed {} files in {:.0f} seconds.'
                      .format(fcnt, toc()))

    print('Processed {} FITS files.'.format(fcnt))
    print('({}) Invalid FITS files encountered: \n\t{}'
          .format(len(badfits), '\n\t'.join(badfits)))


# UNDER CONSTRUCTION
def gdbm_kw_use(topdir, dbfile, progfcnt=10, kwfile=None, fpfile=None):
    """Collect several counts at once"""
    
    with dbm.gnu.open(dbfile,'c') as db:
        # FP:: finger print (SET of keywords in one file)
        kwcnt = Counter() # kwcnt[kw]=count
        fpcnt = Counter() # fpcnt[fpid]=count
        fpkws = dict()    # fpkws[fpid] = set([kw, ...]) # value:: kws
        fpfiles = dict()  # fpfiles[fpid] = [fname, ...]
        fpids = dict()    # fpids[kws] = fpid

        fcnt = 0
        allfits = [f for f in fits_iter(topdir)]
        random.shuffle(allfits)
        warnings.simplefilter('ignore', category=AstropyWarning)
        for fname in allfits:
            fcnt += 1
            kws = kw_set(fname)
            kwcnt.update(kws)
            fpcnt[kws] += 1
            fpids.setdefault(kws, ('fp-{}'.format(fcnt), fname))

            if (fcnt % progfcnt) == 0:
                print('# processed {} files'.format(fcnt))

    print('Processed {} FITS files.'.format(fcnt))

    print('Unique kw Finger Print percent usage: (fp-<numFields>)')
    perc = [('fp-{}'.format(len(k)),v/float(fcnt)) for k,v in fpcnt.items()]
    pprint(sorted(perc,  key=lambda x: x[1], reverse=True),
           stream=fpfile)

    print('Field percent usage:')
    perc = [(k,v/float(fcnt)) for k,v in kwcnt.items()]
    pprint(sorted(perc,  key=lambda x: x[1], reverse=True),
           stream=kwfile)
    
    
def kw_list_list(fitsfname):
    """Return list (file) of list (hdu) of keywords"""
    kws = list()
    hdulist = pyfits.open(fitsfname)
    for hdu in hdulist:
        kws.append(list(hdu.header.keys()))
    hdulist.close()
    return kws

def kw_save(topdir, picklefile, progfcnt=100):
    """Save lists keywords (per file, per HDU) in all FITS under TOPDIR"""
    fcnt=0
    kwll = list()

    for fname in fits_iter(topdir):
        fcnt += 1
        if (fcnt % progfcnt) == 0:
            print('# processed {} files'.format(fcnt))
        kwll.append(kw_list_list(fname))
    #!pprint(kwll)
    with open(picklefile, 'wb') as pf:
        pickle.dump(kwll, pf)
    print('Wrote pickle file: {}'.format(picklefile))
    print('for {} files:'.format(fcnt))
    
##############################################################################

def main():
    "Parse command line arguments and do the work."
    parser = argparse.ArgumentParser(
        description='My shiny new python program',
        epilog='EXAMPLE: %(prog)s a b"'
        )
    parser.add_argument('--version', action='version', version='1.0.1')
    parser.add_argument('fitsdir',
                        help='Path to dir tree containing FITS files')
    parser.add_argument('sqlitedb',
                        help='Save results sqlite3 DB file.')
    parser.add_argument('-k', '--kwcounts',
                        type=argparse.FileType('w'),
                        help='Number of files kw used in.')
    parser.add_argument('-f', '--fpcounts',
                        type=argparse.FileType('w'),
                        help='Number of times each FingerPrint used.')
    parser.add_argument('--loglevel',
                        help='Kind of diagnostic output',
                        choices=['CRTICAL', 'ERROR', 'WARNING',
                                 'INFO', 'DEBUG'],
                        default='WARNING')
    args = parser.parse_args()

    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        parser.error('Invalid log level: %s' % args.loglevel)
    logging.basicConfig(level=log_level,
                        format='%(levelname)s %(message)s',
                        datefmt='%m-%d %H:%M')
    logging.debug('Debug output is enabled in %s !!!', sys.argv[0])

    #kw_histo(args.fitsdir)
    #kw_fingerprints(args.fitsdir)
    #kw_save(args.fitsdir, 'meta.pickle')
    kw_use(args.fitsdir, args.sqlitedb,
           kwfile=args.kwcounts,
           fpfile=args.fpcounts )


if __name__ == '__main__':
    main()
