DVFS: The Distributed Virtual File System

DVFS is a thin wrapper around a few disparate programs and libraries to give a cloud like file system but instead using peer to peer techniques.

Currently this project is in very heavy development, so it isn't recommended to even think about using it until it's further along.

Current installation steps (until this gets rolled into a wheel):
1. Ensure that CouchDB is installed
2. (Most likely inside a virtualenv) install the python requirements with `pip install -r requirements.txt`
3. Upload the database views through `invoke uploadViews`
4. Prepare the database by running `invoke addTestData`

To run the application, from within the dvfs folder run `python dvfs <base> <target>`, where <base> is a permanent base folder and <target> is an empty folder for the filesystem overlay to be applied, and use ctrl+c to quit. If data is added to the target folder it will be reflected in the base folder along with recording it's information in the couchdb database. Data can also be added to the base folder while the application is running and the changes will be reflected in the target folder.
 
 Application components:
 1. Fuse Filesystem: Implemented.
 2. Base file system watcher: Implemented.
 3. File transfer: Under construction.
