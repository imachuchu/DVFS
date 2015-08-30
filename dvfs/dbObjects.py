import couchdbkit as ck
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
        dbView = dbObject(self.dataOb)
        children = dbView.view('dvfs/dbObject-folder',
            key=old,
            classes={'dbFolder':dbFolder, 'dbFile': dbFile}
        )
        for child in children:
            child.delete()
        super(dbFolder, self).delete()


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

    def createNew(self, databaseName, path, mode=S_IFREG, time=False):
        """Creates the file in the database"""
        self.set_db(databaseName)
        if not time:
            time = datetime.utcnow()
        self.createTime = self.accessTime = self.modifyTime = time
        self.st_mode = mode
        self.path = "/" + "/".join(path.split('/')[1:])
        self.save()

        baseFolder = dbObject()
        dbObject.set_db(databaseName)
