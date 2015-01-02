import couchdbkit as ck
from  dbObject import dbObject
from stat import S_IFDIR

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
