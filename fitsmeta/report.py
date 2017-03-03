#! /usr/bin/env python
"""Produce reports(s) on FITS keyword usage.

"""
# EXAMPLES:
# ./report.py ~/data/kwhistos/all-mtn.kwhistos.db

import sys
import argparse
import logging
import sqlite3


def fingerprint_perc(dbfile, minperc=1):
    """Percentage of files that use each FingerPrint."""
    print('FingerPrint use by percentage of files using. (min perc={})'
          .format(minperc))
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    cur.execute('SELECT COUNT(filename) FROM fpfile')
    numfiles = cur.fetchone()[0]

    cur.arraysize = 100
    # Number of files that use each FingerPrint (unique collection of keywords)
    sql = """\
SELECT fpid,COUNT(filename) AS cnt FROM fpfile
GROUP BY fpid ORDER BY cnt DESC"""
    cur.execute(sql)
    best_fpid = None
    for (fpid,count) in cur.fetchmany():
        if count/numfiles < minperc/100:
            break
        print('{:12} {:.1%}'.format(fpid,count/numfiles))
        if not best_fpid:
            best_fpid = fpid
            best_perc = count/numfiles
    print('Keywords in most used FPID ({}, {:.1%}):'
          .format(best_fpid,best_perc))
    cur.execute('SELECT kw,fpid FROM fingerprint WHERE fpid=? ORDER BY kw',
                (best_fpid,))
    for (kw,fpid) in cur.fetchmany():
        print('\t',kw)
        
def keyword_perc(dbfile, minperc=75):
    """Percentage of files that use each KW."""
    print('Keyword use by percentage of files using.  (min perc={})'
          .format(minperc))
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    cur.execute('SELECT COUNT(filename) FROM fpfile')
    numfiles = cur.fetchone()[0]
    print('Total file count: {:,}'.format(numfiles))

    cur.arraysize = 100
    cur.execute('SELECT * FROM kwcount ORDER BY count DESC')
    for (kw,count) in cur.fetchmany():
        if count/numfiles < minperc/100:
            return
        print('{:12} {:.1%}'.format(kw,count/numfiles))
        
    



##############################################################################

def main():
    "Parse command line arguments and do the work."
    parser = argparse.ArgumentParser(
        description='Produce reports(s) on FITS keyword usage.',
        epilog='EXAMPLE: %(prog)s sqlite.db"'
        )
    parser.add_argument('--version', action='version', version='1.0.1')
    parser.add_argument('dbfile', #type=argparse.FileType('r'),
                        help='Input sqlite DB file')
    #!parser.add_argument('outfile', type=argparse.FileType('w'),
    #!                    help='Output output')

    parser.add_argument('--loglevel',
                        help='Kind of diagnostic output',
                        choices=['CRTICAL', 'ERROR', 'WARNING',
                                 'INFO', 'DEBUG'],
                        default='WARNING')
    args = parser.parse_args()
    #!args.outfile.close()
    #!args.outfile = args.outfile.name

    #!print 'My args=',args
    #!print 'infile=',args.infile

    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        parser.error('Invalid log level: %s' % args.loglevel)
    logging.basicConfig(level=log_level,
                        format='%(levelname)s %(message)s',
                        datefmt='%m-%d %H:%M')
    logging.debug('Debug output is enabled in %s !!!', sys.argv[0])


    keyword_perc(args.dbfile)
    fingerprint_perc(args.dbfile)

if __name__ == '__main__':
    main()
