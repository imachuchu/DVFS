import couchdbkit as ck
from stat import S_IFDIR

from  dbObject import dbObject
from dbFile import dbFile

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
