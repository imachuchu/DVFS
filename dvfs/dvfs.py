#!/usr/bin/env python

import logging
import argparse #For easy parsing of the command line arguments

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from datetime import datetime

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
        return 0

    def chown(self, path, uid, gid):
        pass

    def create(self, path, mode):
        """Create the filesystem file"""
        fullPath = self.base + path
        file = open(fullPath, 'w+')
        file.close()

        """Add the CouchDB metadata"""
        newFile = dbFile()
        newFile.set_db(self.database)
        newFile.path = path
        newFile.createTime = newFile.accessTime = newFile.modifyTime = datetime.utcnow()
        newFile.fileHash = sha1('')
        newFile.st_mode = (S_IFREG | mode)
        newFile.size = os.stat(fullPath)
        newFile.save()
        self.fd += 1
        return self.fd
        """
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        """

    def getattr(self, path, fh=None):
        dbView = dbObject(self.dataOb)
        try:
            info = dbView.view('dvfs/dbObject-all',
                key=path,
                classes={'dbFolder':dbFolder, 'dbFile': dbFile}
            ).one()
        except:
            raise FuseOSError(ENOENT)

        #If everything goes fine, but there isn't a record stored
        if not info:
            raise FuseOSError(ENOENT)

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
        import re
        """Create the filesystem folder"""
        fullPath = self.base + path
        os.makedirs(fullPath)

        """Create the CouchDB metadata"""
        newFolder = dbFolder()
        newFolder.set_db(self.database)
        newFolder.path = path
        newFolder.createTime = newFolder.accessTime = newFolder.modifyTime = datetime.utcnow()
        newFolder.st_mode = (S_IFDIR | mode)
        newFolder.st_nlink = 2
        newFolder.save()
        basePath = re.search('(.*)?/.*', path).groups()[0]
        if basePath == '':
            basePath = '/'
        dbView = dbFolder(self.dataOb)
        baseFolder = dbView.view('dvfs/dbFolder-all',
            key=basePath).one()
        baseFolder.st_nlink += 1
        baseFolder.save()

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        from string import split
        from unicodedata import normalize
        dbView = dbObject(self.dataOb)
        documents = dbView.view('dvfs/dbObject-folder',
            key=path,
            classes={'dbFolder': dbFolder, 'dbFile': dbFile}
        ).all()[0]['value']

        def mapPath(documentId):
            path = split(dbView.get(documentId).path, '/')[-1:][0]
            if type(path) is unicode:
                path = normalize('NFKD', path).encode('ascii', 'ignore')
            if path == '':
                return []
            return [path]

        def reducePaths(*args):
            paths = []
            for arg in args:
                paths.extend(arg)
            return paths

        paths = reduce(
            reducePaths,
            map(mapPath, documents),
            ['.', '..']
        )
        logging.debug("before paths output")
        logging.debug(paths)
        return paths

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
