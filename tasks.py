from invoke import run, task

testFiles = [
    {'path': "/test1.txt", 'content': "Test 1's content"},
    {'path': "/test2.txt", 'content': "Test 2's content"}
]
testFolders = [
    {'path': "/"}
]

@task
def addTestData():
    """Adds the test data to the couchdb database"""
    import couchdbkit as ck #For an ORM'ish interface to couchDB
    import os
    from datetime import datetime
    from hashlib import sha1
    from configobj import ConfigObj

    from dvfs.couchdb.dbFile import dbFile

    print("Adding test data")
    server = ck.Server()
    dbName = ConfigObj('config.ini')['dbName']
    database = server.get_or_create_db(dbName)
    os.chdir("dvfs/base")
    for instance in testFiles:
        """File system parts"""
        file = open(instance['path'], 'w+')
        file.write(instance['content'])
        file.close()

        """Couchdb parts"""
        newFile = dbFile()
        newFile.set_db(database)
        newFile.path = instance['path']
        newFile.createTime = datetime.utcnow()
        newFile.accessTime = newFile.createTime
        newFile.modifyTime = newFile.createTime
        newFile.fileHash = sha1(instance['content']).hexdigest()
        newFile.mode = "testMode" #almost assuredly wrong
        newFile.size = os.stat(instance['path']).st_size
        newFile.save()

    for instance in testFolders:
        if not os.path.exists(instance['path']):
            os.makedirs(instance['path']}
        newFolder = dbFolder()
        newFolder.set_db(database)
        newFolder.path = instance['path']
        newFolder.createTime = datetime.utcnow()
        newFolder.accessTime = newFolder.createTime
        newFolder.modifyTime = newFolder.createTime
        newFolder.mode = "testMode" #almost assuredly wrong
        newFolder.save()

@task
def delTestData():
    """Deletes test data from the database"""
    """database.view('dvfs/path-all', params'key="test1.txt"').first()"""
    import couchdbkit as ck #For an ORM'ish interface to couchDB
    import os
    from datetime import datetime
    from hashlib import sha1
    from configobj import ConfigObj

    from dvfs.couchdb.dbFile import dbFile

    server = ck.Server()
    dbName = ConfigObj('config.ini')['dbName']
    os.chdir("dvfs/base")
    database = server.get_db(dbName)

    for instance in testFiles:
        os.remove(instance['path'])

        dbView = dbFile()
        dbView.set_db(database)
        rmFile = dbView.view('dvfs/path-all', key=instance['path']).one() #There should only ever be one anyway
        rmFile.delete()

    print("Deleting test data")

@task
def uploadViews():
    """Syncs all stored views into the couchdb database"""
    import couchdbkit as ck #For an ORM'ish interface to couchDB
    from couchdbkit.designer import push
    from configobj import ConfigObj
    server = ck.Server()
    dbName = ConfigObj('config.ini')['dbName']
    database = server.get_or_create_db(dbName)
    push('dvfs/_design/dvfs', database)
    print("Uploading views")
