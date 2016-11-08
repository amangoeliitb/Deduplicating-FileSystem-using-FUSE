#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import hashlib
import psycopg2

from fuse import FUSE, FuseOSError, Operations

BLOCK_SIZE = 4096
HASH_SIZE = 64

# Database configuration
DATABASE = "fuse"
USER = "postgres"
PASSWORD = "fusepwd"
HOST = "127.0.0.1"
PORT = "5432"


class Passthrough(Operations):
    def __init__(self, root):
        self.root = root

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    # Filesystem methods start here

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(name, self._full_path(target))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)


    def read(self, path, length, offset, fh):
        original_offset = offset
        original_length = length
        offset = (offset * HASH_SIZE) / BLOCK_SIZE
        length = (length * HASH_SIZE) / BLOCK_SIZE
        os.lseek(fh, offset, os.SEEK_SET)
        contents = os.read(fh, length)
        conn = psycopg2.connect(database=DATABASE, user=USER,
                                password=PASSWORD, host=HOST,
                                port=PORT)
        actual_contents = ''
        for i in range(len(contents) / HASH_SIZE):
            block_hash = contents[i * HASH_SIZE:(i + 1) * HASH_SIZE]
            cur = conn.cursor()
            cur.execute("""SELECT * FROM hashes WHERE hash = '%s'""" % (block_hash))
            rows = cur.fetchall()
            actual_contents += rows[0][1]
        length = original_length
        offset = original_offset
        return actual_contents

    def write(self, path, buf, offset, fh):
        conn = psycopg2.connect(database=DATABASE, user=USER,
                                password=PASSWORD, host=HOST,
                                port=PORT)
        new_buf = ''

        original_offset = offset
        offset = (offset * HASH_SIZE) / BLOCK_SIZE
        os.lseek(fh, offset, os.SEEK_SET)
        for i in range(len(buf) / BLOCK_SIZE):
            block_hash = hashlib.sha256(buf[i * BLOCK_SIZE: (i + 1) * BLOCK_SIZE]).hexdigest()
            new_buf += block_hash
            cur = conn.cursor()
            cur.execute("""SELECT * FROM hashes WHERE hash = '%s'""" % (block_hash))
            rows = cur.fetchall()
            if (len(rows) == 0):
                cur.execute("""INSERT INTO hashes VALUES ('%s', '%s')""" % (block_hash, buf[i:i + BLOCK_SIZE]))
            conn.commit()
            cur.close()
        conn.close()
        os.write(fh, new_buf)
        offset = original_offset
        return len(buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])