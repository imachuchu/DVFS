from invoke import run, task


@task
def addTestData():
    """Adds the test data to the couchdb database"""
    print("Adding test data")
