#!/usr/bin/env python

import logging
import argparse #For easy parsing of the command line arguments

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from datetime import datetime
from hashlib import md5

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import couchdbkit as ck
import os

from couchdb.dbObject import dbObject
from couchdb.dbFile import dbFile
from couchdb.dbFolder import dbFolder

if not hasattr(__builtins__, 'bytes'):
    bytes = str

def _addBaseNlink(dataOb, path, amount):
        """Adds amount to the base folder of path, works for both adding and subtracting"""
        dbView = dbFolder(dataOb)
        basePath = os.path.dirname(path)
        basePath = basePath if not basePath == '' else '/'
        baseFolder = dbView.view('dvfs/dbFolder-all', key=basePath).one()
        baseFolder.st_nlink += amount
        baseFolder.save()
        return baseFolder.st_nlink

class dvfs(LoggingMixIn, Operations):
    def __init__(self, base, debug):
        self.files = {}
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
        if self.debug == True:
            logging.debug("in chmod")
        return 0

    def chown(self, path, uid, gid):
        if self.debug == True:
            logging.debug("in chown")
        pass

    def create(self, path, mode):
        """Create the filesystem file"""
        if self.debug == True:
            logging.debug("in create")
        fullPath = self.base + path
        file = open(fullPath, 'w+')
        file.close()

        self._updateFileInfo(path, create=True)

        """Increment the base folder's file descriptor count and return it"""
        return _addBaseNlink(self.dataOb, path, 1)

    def getattr(self, path, fh=None):
        if self.debug == True:
            logging.debug("in getattr")
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
        if self.debug == True:
            logging.debug("in getxattr")
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
        if self.debug == True:
            logging.debug("in listxattr")
        dbView = dbObject(self.dataOb)
        info = dbView.view('dvfs/dbObject-all',
            key=path,
            classes={'dbFolder':dbFolder, 'dbFile': dbFile}
        ).one()
        attrs = info.getAttributes().get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        """Create the filesystem folder"""
        if self.debug == True:
            logging.debug("in mkdir")
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
        _addBaseNlink(self.dataOb, path, 1)

    def open(self, path, flags):
        if self.debug == True:
            logging.debug("in open")
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        if self.debug == True:
            logging.debug("in read")
        fullPath = self.base + path
        with open(fullPath, 'r') as f:
            f.seek(offset)
            value = f.read(size)
        return value

    def readdir(self, path, fh):
        if self.debug == True:
            logging.debug("in readir")
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
        return paths

    def readlink(self, path):
        """This would be nice to have implemented, but it's not necessary"""
        if self.debug == True:
            logging.debug("in readlink")
        raise FuseOSError(ENOENT)
        #return self.data[path]

    def removexattr(self, path, name):
        if self.debug == True:
            logging.debug("in removexattr")
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        """Update the metadata"""
        if self.debug == True:
            logging.debug("in rename")
        dbView = dbObject(self.dataOb)
        couchOb = dbView.view('dvfs/dbObject-all',
            key=old,
            classes={'dbFolder':dbFolder, 'dbFile': dbFile}
        ).one()
        couchOb.path = new
        couchOb.save()
        self._addBaseNlink(old, -1)
        self._addBaseNlink(new, 1)

        """Update the filesystem"""
        fullOldPath = self.base + old
        fullNewPath = self.base + new
        if os.path.exists(fullOldPath):
            os.rename(fullOldPath, fullNewPath)

    def rmdir(self, path):
        """Remove the CouchDB metadata"""
        if self.debug == True:
            logging.debug("in rmdir")
        dbView = dbFolder(self.dataOb)
        folder = dbView.view('dvfs/dbFolder-all', key=path).one()
        folder.delete()
        _addBaseNlink(self.dataOb, path, -1)

        """Delete the base file system folder, if it exists"""
        fullPath = self.base + path
        if os.path.isdir(fullPath):
            os.rmdir(fullPath)

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        if self.debug == True:
            logging.debug("in setxattr")
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        if self.debug == True:
            logging.debug("in statfs")
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        """This would be nice to have implemented, but not necessary"""
        if self.debug == True:
            logging.debug("in symlink")
        raise FuseOSError(ENOENT)
        #self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  #st_size=len(source))

        #self.data[target] = source

    def truncate(self, path, length, fh=None):
        if self.debug == True:
            logging.debug("in truncate")
        fullPath = self.base + path
        with open(fullPath, 'wb+') as fb:
            fb.truncate(length)
        self._updateFileInfo(path)

    def unlink(self, path):
        if self.debug == True:
            logging.debug("in unlink")
        self.files.pop(path)

    def utimens(self, path, times=None):
        if self.debug == True:
            logging.debug("in utimens")
        dbView = dbObject(self.dataOb)
        try:
            info = dbView.view('dvfs/dbObject-all',
                key=path,
                classes={'dbFolder':dbFolder, 'dbFile': dbFile}
            ).one()
        except:
            raise FuseOSError(ENOENT)

        if times:
            if self.debug == True:
                logging.debug(times)
            inTimes = map(datetime.fromtimestamp, times)
        else:
            now = datetime.now()
            inTimes = (now, now)
        info.accessTime, info.modifyTime = inTimes
        info.save()

    def write(self, path, data, offset, fh):
        if self.debug == True:
            logging.debug("In write")
        fullPath = self.base + path
        with open(fullPath, 'wb') as f:
            f.seek(offset)
            f.write(data)
        size = int(os.path.getsize(fullPath))
        self._updateFileInfo(path)
        dbView = dbFile(self.dataOb)
        try:
            if self.debug == True:
                logging.debug("in try statement")
                logging.debug(path)
            info = dbView.view('dvfs/dbFile-all', key=path).one()
        except:
            if self.debug == True:
                logging.debug("in except")
            raise FuseOSError(ENOENT)
        info.size = size
        info.save()
        return len(data)

    def _updateFileInfo(self, path, create = False, mode = False, accessTime = False, modifyTime = False):
        """Updates the couchdb's metadata based on the actual file"""
        fullPath = self.base + path
        dbView = dbFile(self.dataOb)
        info = False
        try:
            info = dbView.view('dvfs/dbFile-all', key=path).one()
        except:
            if self.debug == True:
                logging.debug("file data not found")
        if not info and create:
            info = dbFile()
            info.set_db(self.database)
            info.path = path
            info.createTime = info.accessTime = info.modifyTime = datetime.utcnow()
            info.st_mode = (S_IFREG | mode)
        if info:
            if mode:
                info.st_mode = mode
            if accessTime:
                info.accessTime = accessTime
            if modifyTime:
                info.modifyTime = modifyTime
            info.st_size = os.path.getsize(fullPath)
            info.hash = md5(fullPath).hexdigest()
            info.save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Links a folder to the cloud-ish dvfs filesystem")
    parser.add_argument("base", help="The folder to store local file copies in")
# This argument needs to be last and is actually handled by the fuse module later
    parser.add_argument("target", help="The folder to access the filesystem through")
    parser.add_argument("-f", "--foreground", action="store_true", help="Keep the application in the foreground")
    parser.add_argument("-d", "--debug", action="store_true", help="Activates debug mode")
    args = parser.parse_args()
    if args.debug == True:
        logging.basicConfig(filename='debug.log', level=logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)

    fuse = FUSE(dvfs(args.base, args.debug), args.target, foreground=args.foreground)
