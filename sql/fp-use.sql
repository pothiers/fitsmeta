-- EXAMPLE:
-- sqlite3 -header -column data.db < ../sql/fp-use.sql 

-- Number of files that use each FingerPrint (unique collection of keywords)
SELECT
    fp.fpid,
    count(distinct fpfile.filename) as filecnt --, count(fp.kw) as kwcnt
  FROM fingerprint AS fp, fpfile
  WHERE fpfile.fpid = fp.fpid
  GROUP BY fp.fpid
  ORDER BY filecnt DESC;

