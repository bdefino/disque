# Copyright (C) 2018 Bailey Defino
# <https://bdefino.github.io>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
__package__ = __name__

import db
from db import DB

def _help():
    print "a string-based database\n" \
          "Usage: python db.py [OPTIONS] DIRECTORY ACTION [NAME [DATA]]\n" \
          "OPTIONS\n" \
          "\t-h, --help\tshow this text and exit\n" \
          "DIRECTORY\n" \
          "\tthe database directory\n" \
          "ACTION\n" \
          "\tappend NAME [DATA]\tappend data to an entry\n" \
          "\tclean\tclean the database (if it exists)\n" \
          "\tcontains NAME\tdetermine whether an entry exists\n" \
          "\tdelete NAME\tdelete an entry\n" \
          "\tget NAME\tget an entry\n" \
          "\tinit\tinitialize the database (happens with all actions)\n" \
          "\tlist\tlist all entries as unicode-escaped strings\n" \
          "\tset NAME [DATA]\tset an entry\n" \
          "\tsize\tprint the number of unique entries\n" \
          "NAME\n" \
          "\tan entry name (a CSV string)\n" \
          "DATA\n" \
          "\tentry data (a string)"

def main():
    action = None
    data = ""
    db = None
    directory = None
    name = None
    
    if len(sys.argv) < 3:
        _help()
        sys.exit()

    for arg in sys.argv[1:]:
        if arg in ("-h", "--help"):
            _help()
            sys.exit()
    directory, action = sys.argv[1:3]
    action = action.lower()
    
    if action in ("append", "contains", "delete", "get", "set"):
        if len(sys.argv) < 4:
            print "Missing entry name."
            _help()
            sys.exit()
        name = list(csv.reader(StringIO.StringIO(sys.argv[3])))[0]
        
        if action in ("append", "set") and len(sys.argv) > 4:
            data = sys.argv[4]
    elif not action in ("clean", "init", "list", "size"):
        print "Invalid action."
        _help()
        sys.exit()
    db = DB(directory)
    db.__enter__()
    
    if action == "append":
        db.append(name, data)
    elif action == "clean":
        db.clean()
    elif action == "contains":
        print name in db
    elif action == "delete":
        del db[name]
    elif action == "get":
        sys.stdout.write(db[name])
        sys.stdout.flush()
    elif action == "list":
        for n in db.list():
            if not isinstance(n, list) and not isinstance(n, tuple):
                n = [n]
            csv.writer(sys.stdout).writerow(n)
            sys.stdout.flush()
    elif action == "set":
        db[name] = data
    elif action == "size":
        print len(db)
    # otherwise action == "init"
    db.__exit__()

if __name__ == "__main__":
    main()
