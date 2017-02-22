CREATE TABLE kwcount(
        kw TEXT PRIMARY KEY,
        count INTEGER,
        );
CREATE TABLE fpfile(
        filename TEXT PRIMARY KEY,
        fpid TEXT,
        );
CREATE TABLE fingerprint(
        kw TEXT PRIMARY KEY,
        fpid TEXT,
        );

-- sqlite3 -header -column data.db
SELECT * FROM kwcount ORDER BY count
SELECT * FROM fingerprint ORDER BY fpid
SELECT * FROM fpfile ORDER BY fpid



