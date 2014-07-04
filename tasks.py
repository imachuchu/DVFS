from invoke import run, task

testFiles = [
    {'path': "test1.txt", 'content': "Test 1's content"},
    {'path': "test2.txt", 'content': "Test 2's content"}
]
dbName = 'dvfs'

@task
def addTestData():
    """Adds the test data to the couchdb database"""
    from dvfs.couchdb.dbFile import dbFile
    import couchdbkit as ck #For an ORM'ish interface to couchDB
    import os
    from datetime import datetime
    server = ck.Server()
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
        newFile.save()
    print("Adding test data")

@task
def delTestData():
    """Deletes test data from the database"""
    import os
    os.chdir("dvfs/base")
    for instance in testFiles:
        os.remove(instance['path'])
    print("Deleting test data")
