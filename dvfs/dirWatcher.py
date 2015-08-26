#!/usr/bin/env python

import sys
import time
from hashlib import md5

import couchdbkit as ck
from couchdb.dbObject import dbObject
from couchdb.dbFile import dbFile
from couchdb.dbFolder import dbFolder

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import pudb
import pdb


def startObserver(path='.', database='dvfs'):
	event_handler = dirWatcher(database)
	observer = Observer()
	observer.schedule(event_handler, path, recursive=True)
	observer.start()
	try:
		while True:
			time.sleep(1)
	except KeyboardInterrupt:
		observer.stop()
	observer.join()

class dirWatcher(FileSystemEventHandler):
	def __init__(self, dbName = False):
		if dbName:
			server = ck.Server()
			self.database = server.get_or_create_db(dbName)
			self.dataOb = dbObject.set_db(self.database)
		return super(dirWatcher, self).__init__()

	def on_created(self, event):
		path = "/" + "/".join(event.src_path.split('/')[1:])
		if event.is_directory:
			info = dbFolder()
			info.st_nLink = 2
		else:
			info = dbFile()
		info.set_db(self.database)
		info.path = path
		info.createTime = info.accessTime = info.modifyTime = datetime.utcnow()
		info.st_mode = (S_IFREG)
		info.save()
	def on_deleted(self, event):
		path = "/" + "/".join(event.src_path.split('/')[1:])
		dbView = dbObject(self.dataOb)
		info = dbView.view('dvfs/dbObject-all',
			key=path,
			classes={'dbFolder':dbFolder, 'dbFile': dbFile}
		).one()
		"""Will need more here to recursively delete files/folder"""
		print("On deleted")
	def on_modified(self, event):
		path = "/" + "/".join(event.src_path.split('/')[1:])
		dbView = dbObject(self.dataOb)
		info = dbView.view('dvfs/dbObject-all',
			key=path,
			classes={'dbFolder':dbFolder, 'dbFile': dbFile}
		).one()
		pdb.set_trace()
		info.modifyTime = info.accessTime = datetime.utcnow()
		info.st_size = os.path.getsize(event.src_path)
		info.hash = md5(event.src_path).hexdigest()
		info.save()
	def on_moved(self, event):
		print("On moved")

if __name__ == "__main__":
	print("running from the command line")
	path = sys.argv[1] if len(sys.argv) > 1 else '.'
	startObserver(path)
