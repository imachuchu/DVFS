import couchdbkit as ck
import os
from time import mktime
from stat import S_IFDIR, S_IFLNK, S_IFREG
from datetime import datetime
from hashlib import md5

class dbObject(ck.Document):
    """Class that all objects will inherit. Used for common fields and settings"""
    modifyTime = ck.DateTimeProperty()
    createTime = ck.DateTimeProperty()
    accessTime = ck.DateTimeProperty()
    path = ck.StringProperty() #contains the full path of the object
    st_mode = ck.IntegerProperty()

    def _getBaseAttributes(self):
        """Sets up base attributes to be added to by children classes
            Not to be called on it's own (use the children's getAttributes instead)
        """
        returnStat = dict()
        returnStat['st_atime'] = mktime(self.accessTime.timetuple())
        returnStat['st_mtime'] = mktime(self.modifyTime.timetuple())
        returnStat['st_ctime'] = mktime(self.createTime.timetuple())
        return returnStat


class dbFolder(dbObject):
    """A folder for holding other folders and files
        Nothing may need to be added here, the base attributes may be enough
        Identified by the railing slash in the path
    """
    st_nlink = ck.IntegerProperty()

    def getAttributes(self):
        """Converts the database version into FUSE ready attributes"""
        returnStat = self._getBaseAttributes()
        returnStat['st_mode'] = (S_IFDIR | 0755)
        returnStat['st_nlink'] = self.st_nlink
        return returnStat

    def delete(self):
        """Since there might be other files and folders underneath this folder we have to handle deleting them before ourselves"""
        dbView = dbObject()
        dbView.set_db(self.get_db())
        children = dbView.view('dvfs/dbObject-folder',
            key=self.path,
            classes={'dbFolder':dbFolder, 'dbFile': dbFile}
        )
        for child in children:
            child.delete()
        basePath = os.path.split(self.path)[0]
        baseFolder = dbView.view('dvfs/dbObject-all',
            key=basePath,
            classes={'dbFolder':dbFolder}
        ).one()
        baseFolder.st_nlink -= 1
        baseFolder.save()
        super(dbFolder, self).delete()

    def createNew(self, dbName, path, basePath=False, mode=False, time=False):
        """Creates a new folder in the named database"""
        server = ck.Server()
        database = server.get_or_create_db(dbName)
        self.set_db(database)
        if not time:
            time = datetime.utcnow()
        self.createTime = self.accessTime = self.modifyTime = time
        if not mode:
            mode = S_IFDIR
        self.st_mode = mode
        self.st_nlink = 2
        self.path = path
        self.save()

        dbView = dbFolder()
        dbView.set_db(database)
        folPath = os.path.split(path)
        baseFolder = dbView.view('dvfs/dbFolder-all', key=folPath[0]).one()
        """There should always be a base folder, if not then there's a problem"""
        baseFolder.st_nlink += 1
        baseFolder.save()


class dbFile(dbObject):
    """The metadata of a stored file"""
    st_size = ck.IntegerProperty() #The size of the file
    fileHash = ck.StringProperty() #File hash, calculated using hashlib. Emptrystring if file is empty

# More fields that may be implemented later
    #oldHash = ck.StringProperty() #Look at older version of record
    #parent = ck.StringProperty(), may not be needed actually

    def getAttributes(self):
        """Converts the database version into FUSE ready attributes"""
        returnStat = self._getBaseAttributes()
        returnStat['st_mode'] = (S_IFREG | 0755)
        returnStat['st_nlink'] = 1
        #returnStat['st_size'] = 16 # Will need to be set to actual file size when they get written
        returnStat['st_size'] = self.st_size
        return returnStat

    def createNew(self, dbName, path, basePath=False, mode=False, time=False):
        """Creates the file in the database"""
        server = ck.Server()
        database = server.get_or_create_db(dbName)
        self.set_db(database)
        self.path = path
        if not time:
            time = datetime.utcnow()
        self.createTime = self.accessTime = self.modifyTime = time
        if not mode:
            mode = S_IFREG
        self.st_mode = mode
        if basePath:
            self.hash = md5(basePath).hexdigest()
            self.st_size = os.path.getsize(basePath)
        self.save()

        dbView = dbFolder()
        dbView.set_db(database)
        folPath = os.path.split(path)
        baseFolder = dbView.view('dvfs/dbFolder-all', key=folPath[0]).one()
        """There should always be a base folder, if not then there's a problem"""
        baseFolder.st_nlink += 1
        baseFolder.save()

    def delete(self):
        """Deletes the file and decrement's it's base folders link list"""
        dbView = dbFolder()
        dbView.set_db(self.get_db)
        basePath = os.path.split(path)[0]
        baseFolder = dbView.view('dvfs/dbObject-all',
            key=basePath).one()
        baseFolder.st_nlink -= 1
        super(dbFile, self).delete()
