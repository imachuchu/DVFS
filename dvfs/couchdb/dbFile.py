import couchdbkit as ck
import stat    # for file properties
from dbObject import dbObject

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
        returnStat['st_mode'] = (stat.S_IFREG | 0755)
        returnStat['st_nlink'] = 1
        #returnStat['st_size'] = 16 # Will need to be set to actual file size when they get written
        returnStat['st_size'] = self.st_size
        return returnStat
