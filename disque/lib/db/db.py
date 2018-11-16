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
import csv
import fcntl
import hashlib
import os
import StringIO
import sys

from lib import withfile

__doc__ = "a string-based database"

def _as_list(var):
    """return a variable as a list, but don't split strings"""
    if isinstance(var, tuple):
        var = list(var)
    elif not isinstance(var, list):
        var = [var]
    return var

class DB:
    """
    an extensible string-based database
    
    the database is made up of two main components:
    1. the database file
        a ragged CSV document where each row is a list of name components
    2. entries
        the corresponding raw data for an entry name,
        stored at "directory/subtree of hashed name components/entry.dat"

    the database model is dict-like, but is intended for extensibility
    through its simplicity

    the functions follow a simple model for integrity purposes:
    1. enter as needed
    2. open any files
    3. lock files
    4. save any exceptions during the operation
    5. unlock files
    6. close any files
    7. raise any exceptions
    8. return any data
    """
    
    def __init__(self, directory = os.getcwd(), hash = "sha256"):
        self.directory = os.path.realpath(directory)
        self._fp = None
        hash = getattr(hashlib, hash)
        self._hash = lambda s: hash(str(s)).hexdigest()
        self.path = os.path.join(self.directory, "db.csv")
        self._reader = None
        self._writer = None

    def append(self, name, data, offset = 0, whence = os.SEEK_CUR,
            truncate = False):
        """append data to an entry"""
        new = not name in self
            
        with DBEntry(self._generate_path(name)) as entry:
            entry.append(data, offset, whence, truncate)

            if entry.new:
                self.register(name)

    def clean(self, filter = lambda n: True):
        """filter entries in the database file"""
        self.__enter__()

        with withfile.FileLock(self._fp):
            names = self.list()
            self._fp.seek(0, os.SEEK_SET)
            self._fp.truncate()

            for n in names:
                if filter(n):
                    self.register(n)
    
    def __contains__(self, name):
        """return whether an entry exists"""
        with withfile.FileLock(self._fp):
            return os.path.exists(self._generate_path(name))

    def __del__(self):
        self.__exit__()

    def __delitem__(self, name):
        """delete an entry"""
        with withfile.FileLock(self._fp):
            with DBEntry(self._generate_path(name)) as entry:
                entry.delete()
            self.deregister(name)

    def deregister(self, name):
        """deregister a name from the database file"""
        name = _as_list(name) # do this only once
        self.clean(lambda n: not n == name)
    
    def __enter__(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        
        if not isinstance(self._fp, file) or self._fp.closed:
            self._fp = open(self.path, "a+b")
            self._reader = csv.reader(self._fp)
            self._writer = csv.writer(self._fp)
        return self

    def existent(self):
        """remove redundant/nonexistent entries from the database file"""
        self.clean(lambda n: os.path.exists(self._generate_path(n)))

    def __exit__(self, *exception):
        if isinstance(self._fp, file):
            try:
                self._fp.close()
            except (IOError, OSError):
                pass

    def _generate_path(self, name):
        """return the hashed equivalent of a name (None is evaluated as "")"""
        if not isinstance(name, list) and not isinstance(name, tuple):
            name = [name]
        else:
            name = list(name)

        for i, n in enumerate(name):
            if n == None:
                n = ""
            name[i] = self._hash(n)
        return os.path.join(os.path.realpath(self.directory), *name)

    def __getitem__(self, name):
        """retrieve an entry"""
        with DBEntry(self._generate_path(name)) as entry:
            return entry.get()

    def __len__(self):
        """return the number of unique entries"""
        return len(self.list())
    
    def list(self):
        """return a sorted list of all the entry names"""
        self.__enter__()
        names = set()
        
        with withfile.FileLock(self._fp):
            self._fp.seek(0, os.SEEK_SET)
            
            for l in self._reader:
                if l in self:
                    names.add(tuple(l))
        return sorted((list(n) for n in names))
    
    def register(self, name):
        """register a name with the database"""
        self.__enter__()

        with withfile.FileLock(self._fp):
            self._fp.seek(0, os.SEEK_END)
            self._writer.writerow(_as_list(name))
            self._fp.flush()
            os.fdatasync(self._fp.fileno())
    
    def __setitem__(self, name, data):
        """store a name mapped to data"""
        self.append(name, data, truncate = True, whence = os.SEEK_SET)

    def traverse(self, open = False):
        """
        generate DBEntry instances while performing
        an unordered traversal of the database
        """
        self.__enter__()

        with withfile.FileLock(self._fp):
            self._fp.seek(0, os.SEEK_SET)

            for name in self._reader:
                yield DBEntry(self._generate_path(name))

class DBEntry:
    """
    I/O on a raw database entry

    the entry is stored in in the provided directory,
    and assumes the given name

    an entry is composed as such:
        entry-directory/
            entry.dat
    where entry-directory is a unique directory,
    and entry.dat contains raw data
    """

    def __init__(self, directory):
        self.directory = directory
        self._fp = None # data file pointer
        self.path = os.path.join(self.directory, "entry.dat")
        self.new = not os.path.exists(self.path) # whether the entry is new

    def append(self, data, offset = 0, whence = os.SEEK_CUR, truncate = False):
        """
        append data to the entry

        this function is rather slow, as it calls
        both file.flush and os.fdatasync
        """
        self.__enter__()

        with withfile.FileLock(self._fp):
            self._fp.seek(offset, whence)
            self._fp.write(data)
            self._fp.flush()
            os.fdatasync(self._fp.fileno())

            if truncate:
                self._fp.truncate()
                self._fp.flush() # just in case, we have to re-sync
                os.fdatasync(self._fp.fileno())

    def delete(self, rmemptydirs = True):
        """delete the entry and optionally all empty parent directories"""
        self.__enter__()

        with withfile.FileLock(self._fp):
            os.unlink(self.path)

            if rmemptydirs:
                dir = self.directory

                while not dir in ("", os.sep) and not os.listdir(dir):
                    os.rmdir(dir)
                    dir = os.path.dirname(dir)

    def __enter__(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        if not isinstance(self._fp, file) or self._fp.closed:
            mode = "w+b"

            if os.path.exists(self.path):
                mode = "r+b"
            self._fp = open(self.path, mode)
        return self

    def __exit__(self, *exception):
        if isinstance(self._fp, file):
            try:
                self._fp.close()
            except (IOError, OSError):
                pass

    def get(self, offset = 0, whence = os.SEEK_SET):
        """get the entry's data"""
        self.__enter__()
        data = ""

        with withfile.FileLock(self._fp):
            start = self._fp.tell()
            self._fp.seek(offset, whence)
            data = self._fp.read()
            self._fp.seek(start, os.SEEK_SET)
        return data
    
    def set(self, data):
        """set the entry's data"""
        self.append(data, truncate = True, whence = os.SEEK_SET)
