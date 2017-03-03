CREATE TABLE kwcount(
        kw TEXT PRIMARY KEY,
        count INTEGER,
        );
CREATE TABLE fpfile(
        filename TEXT PRIMARY KEY,
        fpid TEXT,
        );
CREATE TABLE fingerprint(
        kw TEXT,
        fpid TEXT,
        PRIMARY KEY (kw, fpid),
        );

-- sqlite3 -header -column data.db
SELECT * FROM kwcount ORDER BY count;
SELECT * FROM fingerprint ORDER BY fpid;
SELECT * FROM fpfile ORDER BY fpid;

-- These two should return same count
-- chimp:~/data
SELECT count(distinct fpid) FROM fpfile ORDER BY fpid;
-- => 35
SELECT count(distinct fpid) FROM fingerprint ORDER BY fpid;
-- => 35

SELECT COUNT(filename) FROM fpfile;
-- max value for kwcount.count

-- sqlite3
    .schema
    .header ON
    .mode column

-- sqlite ~/data/kwhistos/all-mtn.kwhistos.db
