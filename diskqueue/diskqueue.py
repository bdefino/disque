# Copyright (C) 2018 Bailey Defino
# <https://hiten2.github.io>

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
import hashlib
import os
import Queue
import sys
import thread
import time

from lib import db
from lib import threaded

__doc__ = "persistent queueing"

class DiskQueue:
    """
    a disk-based FIFO structure intended to alleviate memory-hungry queuing
    
    essentially, this is a linked list of chunked data stored within
    a database

    this maintains queue order by NOT
    """
    ###########################################synchonization
    ##########################################error handling
    NEXT_PATH = "next path"
    
    def __init__(self, directory = os.getcwd(), hash = "sha256",
            n_per_chunk = 512):
        self._db = db.DB(directory, hash)
        self._get_fp = None
        self._get_fp_reader = None
        self._get_fp_size = 0
        self._next_path = None

        if DiskQueue.NEXT_PATH in self._db:
            self.next_path = self._db[DiskQueue.NEXT_PATH]
        self._put_fp = None
        self._put_nlines = 0

    def __enter__(self):
        self._db.__enter__()
        return self

    def __exit__(self, *exception):
        for fp in (self._get_fp, self._put_fp):
            if isinstance(fp, file) and not fp.closed:
                fp.close()
        self._db.__exit__(*exception)
    
    def get(self):
        """get the next available value"""
        self.__enter__()
        
        if not isinstance(self._get_fp, file):
            if self._next_path == None:
                if not DiskQueue.NEXT_PATH in self._db:
                    raise ValueError("empty")
                self._next_path = self._db[DiskQueue.NEXT_PATH]
            self._get_fp = open(self._next_path)
            self._get_fp.seek(0, os.SEEK_END)
            self._get_fp_size = self._get_fp.tell()
            self._get_fp.seek(0, os.SEEK_SET)
            self._get_fp_reader = reader.

        try:
            value = csv.reader(
        ###############read next line
        ###############check for EOF

    def put(self):
        pass
