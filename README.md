# Deduplicating-FileSystem-using-FUSE

### Description:
The file `fs.py` implements a very basic deduplicating filesystem using FUSE.
You can read about deduplicating file systems on [Wikipedia](https://en.wikipedia.org/wiki/Data_deduplication).
The idea is to reduce the amount of disk space used by the file system in the case where several blocks of data occur frequently.

We have used Postgres to store repeating blocks with their SHA256 hash as the key.
This filesystem is purely for learning purposes and so, we have ignored collisions.
A block of 4096 bytes is compressed to 64 bytes. So, the compression factor is about 4096 / 64 = 64.
Of course, we need to account for the database storage cost

### Setup:
- Install FUSE for Python: `sudo pip install fusepy`
- Install psycopg2 (Postgres library) for Python: `sudo pip install psycopg2`
- Setup database and table in postgres as follows:
  - `CREATE DATABASE fuse;`
  - `CREATE TABLE hashes(hash VARCHAR(64) PRIMARY KEY, block CHAR(4096));`


### Important Note:
Care has to be taken that reads and writes are all done in multiples of 4096
