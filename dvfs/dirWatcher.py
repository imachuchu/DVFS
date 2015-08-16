#!/usr/bin/env python

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import sys
import time

def main():

	path = sys.argv[1] if len(sys.argv) > 1 else '.'
	event_handler = dirWatcher()
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
	def on_created(self, event):
		print("On Created")
	def on_deleted(self, event):
		print("On deleted")
	def on_modified(self, event):
		print("On modified")
	def on_moved(self, event):
		print("On moved")

if __name__ == "__main__":
	print("running from the command line")
	main()
