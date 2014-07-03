#!/usr/bin/env python
import argparse #For easy parsing of the command line arguments
import logging #For debug/error logging

def main():
    print("In the main function")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Links a folder to the cloud-ish filesystem")
    parser.add_argument("base", help="The folder to store local file copies in")
# This argument needs to be last and is actually handled by the fuse module later
    parser.add_argument("target", help="The folder to access the filesystem through")
    parser.add_argument("-d", "--debug", action="store_true", help="Activates debug mode")
    args = parser.parse_args()
    if args.debug == True:
        logging.basicConfig(filename='debug.log', level=logging.DEBUG)
    main()
