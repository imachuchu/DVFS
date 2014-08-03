#!/usr/bin/env python
from fuse import FUSE, Operations, FuseOSError, LoggingMixIn
import argparse #For easy parsing of the command line arguments
import logging #For debug/error logging
import couchdbkit as ck
import os
from couchdb.dbObject import dbObject
from couchdb.dbFile import dbFile
from couchdb.dbFolder import dbFolder

class dvfs(LoggingMixIn, Operations):

    def __init__(self, base, debug):
        self.debug = debug
        if self.debug == True:
            logging.debug("init, base = %s" % base)
        if base[0] == '/':
            self.base = base
        else:
            self.base = os.path.dirname(os.path.realpath(__file__)) + "/" + base
        if self.debug == True:
            logging.debug("init done, self.base = %s" % self.base)
        self.connectDatabase(dbName='dvfs')

    def connectDatabase(self,dbName):
        """Sets up the database to be interacted"""
        server = ck.Server()
        self.database = server.get_or_create_db(dbName)
        self.dataOb = dbObject.set_db(server.get_or_create_db(dbName))

    def getattr(self, path, fh=None):
        """Returns the attributes of the specific file"""
        if self.debug == True:
            logging.debug("Inside the getattr function, path= %s" % path)
        if path == '/':
            now = time()
            return dict(st_mode=(stat.S_IFDIR | 0755), st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2)
        dbView = dbFile(self.dataOb)
        fileInfo = dbView.view('dvfs/filePath', key=path).one() #There should only ever be one anyway
        if fileInfo is None:
            raise FuseOSError(ENOENT)
        else:
            return fileInfo.getAttributes()

    def readdir(self, path, fh=None):
        """Returns the attributes of a folder"""
        if self.debug == True:
            logging.debug("Inside the readdir function, path= %s" % path)
        direntry = ['.', '..'] 
        dbView = dbFile(self.dataOb)
        fileList = dbView.view('dvfs/folderPath', key=path)
        for entry in fileList:
            direntry.append(entry.path[len(path):].encode('utf-8')) #A bit ugly, but will always work by definition
        return direntry

    def read(self, path, size, offset, fh=None):
        """Loads data from an opened file"""
        if self.debug == True:
            logging.debug("the read path is= " + path)
        return ''

    def open(self, path, flags=None):
        """For checking permission of pending file
        Needs to be implemented to pull down files that we don't have"""
        if self.debug == True:
            logging.debug("the open path is= " + path)
        return -errno.EACCES

    def write(self, path, data, offset, fh=None):
        #Update the database metadata
        """Actually writes data into the file"""
        if self.debug == True:
            logging.debug("In the write call")
        return 0

# These features aren't implemented yet, and some may never be
    def readlink(self, path):
        """Reads a symbolic link
        May be implemented
        """
        if self.debug == True:
            logging.debug("In readlink, path= %s" % path)
        pass

    def unlink(self, path):
        """Removes a file"""
        if self.debug == True:
            logging.debug("In unlink, path = %s" % path)
        pass

    def rmdir(self, path):
        """Removes a directory
        Will need to be implemented
        """
        if self.debug == True:
            logging.debug("In rmdir, path = %s" % path)
        pass

    def symlink(self, path, path1):
        """Creates a symbolic path
        May be implemented
        """
        if self.debug == True:
            logging.debug("In symlink, path = %s" % path)
        pass

    def link(self, path, path1):
        """Creates a hard link
        May be implemented
        """
        if self.debug == True:
            logging.debug("In link, path = %s" % path)
        pass

    def chmod(self, path, mode):
        """Changes the permissions on a file
        Will need to be implemented, and will be complex
        """
        if self.debug == True:
            logging.debug("In chmod, path = %s" % path)
        return 0

    def chown(self, path, mode):
        """Changes the owner/group of a file
        Will need to be implemented
        """
        if self.debug == True:
            logging.debug("In chown, path = %s" % path)

    def truncate(self, path, length, fh=None):
        """Changes the size of a file
        Should be easy to implement
        """
        if self.debug == True:
            logging.debug("In truncate, path = %s" % path)
        pass

    def mknod(self, path, mode, dev):
        """Creates a new file
        dev can be safely ignored
        """
        if self.debug == True:
            logging.debug("In mknod, path = %s" % path)
        # Create the file in the database
        pass

    def mkdir(self, path, mode):
        """Makes a new directory
        """
        if self.debug == True:
            logging.debug("In mkdir, path = %s" % path)

    def utimens(self, path, times=None):
        """Changes the modification/access time of a file
        path = path to file
        atime = access time
        mtime = modification time
        """
        if self.debug == True:
            logging.debug("In utimens, path = %s" % path)

    def access(self, path, mode):
        """Checks if a file can be accessed by the user
        Not an issue for us
        Will not be implemented
        """
        if self.debug == True:
            logging.debug("In access, path = %s" % path)
        pass

    def statfs(self, path):
        """Return filesystem information
        Since this isn't a real filesystem we really don't have anything valid to return
        Will not be implemented
        """
        if self.debug == True:
            logging.debug("In statfs, path = %s" % path)
        pass

    def fsinit(self):
        """Called when the filesystem is ready to serve requests.
        Often used for spawning threads and such
        Will need to be implemented to:
            Calculate current local file hashes
            Commit/update them to the newest version available
        """
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Links a folder to the cloud-ish dvfs filesystem")
    parser.add_argument("base", help="The folder to store local file copies in")
# This argument needs to be last and is actually handled by the fuse module later
    parser.add_argument("target", help="The folder to access the filesystem through")
    parser.add_argument("-d", "--debug", action="store_true", help="Activates debug mode")
    args = parser.parse_args()
    if args.debug == True:
        logging.basicConfig(filename='debug.log', level=logging.DEBUG)

    fuse = FUSE(dvfs(args.base, args.debug), args.target, foreground=False, nothreads=True)
