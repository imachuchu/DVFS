from invoke import run, task

@task
def addTestData():
    """Adds the test data to the couchdb database"""
    from dvfs.couchdb.dbFile import dbFile
    import os
    files = [
        {'name': "test1.txt", 'content': "Test 1's content"},
        {'name': "test2.txt", 'content': "Test 2's content"}
    ]
    os.chdir("dvfs/base")
    for instance in files:
        file = open(instance['name'], 'w+')
        file.write(instance['content'])
        file.close()


    print("Adding test data")
