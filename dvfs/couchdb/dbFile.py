import couchdbkit as ck
import stat    # for file properties
from dbObject import dbObject

class dbFile(dbObject):
    """The metadata of a stored file"""
    size = ck.IntegerProperty()
    fileHash = ck.StringProperty() #Calculate using hashlib

# More fields that may be implemented later
    #oldHash = ck.StringProperty() #Look at older version of record
    #parent = ck.StringProperty(), may not be needed actually

    def getAttributes(self):
        """Converts the database version into FUSE ready attributes"""
        returnStat = self._getBaseAttributes()
        returnStat['st_mode'] = (stat.S_IFREG | 0755)
        returnStat['st_nlink'] = 1
        returnStat['st_size'] = 16 # Will need to be set to actual file size when they get written
        return returnStat
