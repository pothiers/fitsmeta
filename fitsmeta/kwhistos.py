#! /usr/bin/env python
"""Produce histograms of FITS header keyword usage
"""
# EXAMPLE:
#  python3 ./kwhistos.py ~/data  data.pkl



import sys
import argparse
import logging
import glob
import os.path
import pickle
import itertools
#!import yaml
import warnings

import astropy.io.fits as pyfits
from astropy.utils.exceptions import AstropyWarning
from collections import Counter
from pprint import pprint

def kw_set(fitsfname):
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
    return frozenset(kws)

def fits_iter(topdir):
    gfz = os.path.join(topdir,'**','*.fz')
    gfits = os.path.join(topdir,'**','*.fits')
    return(itertools.chain(glob.iglob(gfz, recursive=True),
                           glob.iglob(gfits, recursive=True)))


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
    # Remove keys that can have duplicates
    del cnt['COMMENT']
    del cnt['HISTORY']
    del cnt['']
    
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

def kw_use(topdir, picklefile, progfcnt=10, kwfile=None, fpfile=None):
    """Collect several counts at once"""
    kwcnt = Counter() # count keywords
    fpcnt = Counter() # count finger prints (unique set of keywords in one file)
    fcnt=0
    fps = dict()
    all = dict()
    warnings.simplefilter('ignore', category=AstropyWarning)
    for fname in fits_iter(topdir):
        fcnt += 1
        if (fcnt % progfcnt) == 0:
            print('# processed {} files'.format(fcnt))
        kws = kw_set(fname)
        kwcnt.update(kws)
        fpcnt[kws] += 1
        all[fname] = kws

    print('Processed {} FITS files.'.format(fcnt))

    print('Unique kw Finger Print percent usage: (fp-<numFields>)')
    perc = [('fp-{}'.format(len(k)),v/float(fcnt)) for k,v in fpcnt.items()]
    pprint(sorted(perc,  key=lambda x: x[1], reverse=True),
           stream=fpfile)

    print('Field percent usage:')
    perc = [(k,v/float(fcnt)) for k,v in kwcnt.items()]
    pprint(sorted(perc,  key=lambda x: x[1], reverse=True),
           stream=kwfile)

    print('Save kw set (union over HDU) from each file into: {}'
          .format(picklefile))
    with open(picklefile, 'wb') as pf:
        pickle.dump(all, pf)

    
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
    #!yamlfile = picklefile + '.yaml'
    #!with open(yamlfile,'w') as yf:
    #!    yaml.dump(kwll, yf, default_flow_style=False)
    #!print('Wrote yaml file: {}'.format(yamlfile))
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
    parser.add_argument('picklefile',
                        help='Save kw lists in this pickle file.')
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
    kw_use(args.fitsdir, args.picklefile,
           kwfile=args.kwcounts,
           fpfile=args.fpcounts )


if __name__ == '__main__':
    main()
