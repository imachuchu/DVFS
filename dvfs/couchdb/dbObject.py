import couchdbkit as ck
from time import mktime

class dbObject(ck.Document):
    """Class that all objects will inherit. Used for common fields and settings"""
    modifyTime = ck.DateTimeProperty()
    createTime = ck.DateTimeProperty()
    accessTime = ck.DateTimeProperty()
    path = ck.StringProperty() #contains the full path of the object
    mode = ck.StringProperty()

    def _getBaseAttributes(self):
        """Sets up base attributes to be added to by children classes
            Not to be called on it's own (use the children's getAttributes instead)
        """
        returnStat = dict()
        returnStat['st_atime'] = mktime(self.accessTime.timetuple())
        returnStat['st_mtime'] = mktime(self.modifyTime.timetuple())
        returnStat['st_ctime'] = mktime(self.createTime.timetuple())
        return returnStat
