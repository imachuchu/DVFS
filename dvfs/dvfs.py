#!/usr/bin/env python

import logging
import argparse #For easy parsing of the command line arguments

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import couchdbkit as ck
import os

from couchdb.dbObject import dbObject
from couchdb.dbFile import dbFile
from couchdb.dbFolder import dbFolder

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class dvfs(LoggingMixIn, Operations):
    def __init__(self, base, debug):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)

        """dvfs stuff"""
        self.debug = debug
        if base[0] == '/':
            self.base = base
        else:
            self.base = os.path.dirname(os.path.realpath(__file__)) + "/" + base
        self.connectDatabase(dbName='dvfs')

    def connectDatabase(self,dbName):
        """Sets up the database to be interacted"""
        if self.debug == True:
            logging.debug("connecting to database")
        server = ck.Server()
        self.database = server.get_or_create_db(dbName)
        self.dataOb = dbObject.set_db(self.database)

    def chmod(self, path, mode):
        self.files[path]['st_mode'] &= 0770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

    def create(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        if path not in self.files:
            raise FuseOSError(ENOENT)

        dbView = dbObject(self.dataOb)
        info = dbView.view('dvfs/dbObject-all',
            key=path,
            classes={'dbFolder':dbFolder, 'dbFile': dbFile}
        ).one()

        return info.getAttributes()

    def getxattr(self, path, name, position=0):
        dbView = dbObject(self.dataOb)
        info = dbView.view('dvfs/dbObject-all',
            key=path,
            classes={'dbFolder':dbFolder, 'dbFile': dbFile}
        ).one()

        attrs = info.getAttributes().get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        dbView = dbObject(self.dataOb)
        info = dbView.view('dvfs/dbObject-all',
            key=path,
            classes={'dbFolder':dbFolder, 'dbFile': dbFile}
        ).one()
        attrs = info.getAttributes().get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

        self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        dbView = dbObject(self.dataOb)
        documents = dvView('dvfs/dbObject-folder',
            key=path,
            classes={'dbFolder': dbFolder, 'dbFile': dbFile}
        ).all()
        paths = []
        for document in documents:
            name = paths.append(dbView.get(document).path)[-1:]

        return ['.', '..'] + [x[1:] for x in self.files if x != '/']

    def readlink(self, path):
        return self.data[path]

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))

        self.data[target] = source

    def truncate(self, path, length, fh=None):
        self.data[path] = self.data[path][:length]
        self.files[path]['st_size'] = length

    def unlink(self, path):
        self.files.pop(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        self.data[path] = self.data[path][:offset] + data
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Links a folder to the cloud-ish dvfs filesystem")
    parser.add_argument("base", help="The folder to store local file copies in")
# This argument needs to be last and is actually handled by the fuse module later
    parser.add_argument("target", help="The folder to access the filesystem through")
    parser.add_argument("-d", "--debug", action="store_true", help="Activates debug mode")
    args = parser.parse_args()
    if args.debug == True:
        logging.basicConfig(filename='debug.log', level=logging.DEBUG)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(dvfs(args.base, args.debug), args.target, foreground=False)
