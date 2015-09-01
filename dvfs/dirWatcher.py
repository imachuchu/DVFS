#!/usr/bin/env python

import sys
import time
import os
from datetime import datetime
from hashlib import md5
from stat import S_IFDIR, S_IFLNK, S_IFREG

import couchdbkit as ck
from dbObjects import dbObject, dbFile, dbFolder

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


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
			self.dbName = dbName
		return super(dirWatcher, self).__init__()

	def on_created(self, event):
		path = "/" + "/".join(event.src_path.split('/')[1:])
		if event.is_directory:
			info = dbFolder()
		else:
			info = dbFile()
		info.createNew(self.dbName, path, event.src_path)
	def on_deleted(self, event):
		path = "/" + "/".join(event.src_path.split('/')[1:])
		dbView = dbObject(self.dataOb)
		info = dbView.view('dvfs/dbObject-all',
			key=path,
			classes={'dbFolder':dbFolder, 'dbFile': dbFile}
		).one()
		if info:
			info.delete()
	def on_modified(self, event):
		path = "/" + "/".join(event.src_path.split('/')[1:])
		if event.is_directory:
			dbView = dbFolder(self.dataOb)
		else:
			dbView = dbFile(self.dataOb)
		info = dbView.view('dvfs/dbObject-all',
			key=path
		).one()
		info.modifyTime = info.accessTime = datetime.utcnow()
		if not event.is_directory:
			info.st_size = os.path.getsize(event.src_path)
			info.fileHash = md5(event.src_path).hexdigest()
		info.save()
	def on_moved(self, event):
		srcPath = "/" + "/".join(event.src_path.split('/')[1:])
		destPath = "/" + "/".join(event.dest_path.split('/')[1:])
		dbView = dbObject(self.dataOb)
		record = dbView.view('dvfs/dbObject-all',
			key=srcPath,
			classes={'dbFolder':dbFolder, 'dbFile': dbFile}
		).one()
		record.path=destPath
		record.save()

if __name__ == "__main__":
	print("running from the command line")
	path = sys.argv[1] if len(sys.argv) > 1 else '.'
	startObserver(path)
