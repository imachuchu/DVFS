from invoke import run, task

testFiles = [
    {'path': "test1.txt", 'content': "Test 1's content"},
    {'path': "test2.txt", 'content': "Test 2's content"}
]


@task
def addTestData():
    """Adds the test data to the couchdb database"""
    from dvfs.couchdb.dbFile import dbFile
    import os
    os.chdir("dvfs/base")
    for instance in testFiles:
        file = open(instance['name'], 'w+')
        file.write(instance['content'])
        file.close()
    print("Adding test data")

@task
def delTestData():
    """Deletes test data from the database"""
    import os
    os.chdir("dvfs/base")
    for instance in testFiles:
        os.remove(instance['path'])
    print("Deleting test data")
