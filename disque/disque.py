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

from lib import threaded
from lib import withfile

__doc__ = "persisent queueing"

class Disque:
    """
    a disk-based FIFO structure intended to alleviate memory-hungry queuing
    
    essentially, this is a linked list of chunked data stored within
    a database

    the queue operates within a directory-based database,
    and maintains persistence and synchronization
    via 2 files used in tandem with flock calls

    each chunk is stored as an array of values,
    and terminated with the name of the next expected chunk;
    these are stored in the CSV format

    the dequeue operation skips any leading NUL bytes,
    then overwrites the dequeued CSV row with the NUL byte,
    in order to maintain persistence

    note that __enter__ and __exit__ aren't thread-safe
    """
    ###########################################synchonization
    ##########################################error handling

    class Dialect(csv.excel):
        quoting = csv.QUOTE_ALL
    
    HEAD_INDEX = "head-index"
    TAIL_INDEX = "tail-index"
    
    def __init__(self, directory = os.getcwd(), hash = "sha256",
            n_per_chunk = 512):
        self.directory = directory
        self._get_lock = thread.allocate_lock()
        self._head_fp = None
        self._head_fp_reader = None
        self._head_fp_size = None
        self._head_index_fp = None
        self.hash = hash
        self.n_per_chunk = n_per_chunk
        self._put_lock = thread.allocate_lock()
        self._tail_fp = None
        self._tail_fp_nlines = 0
        self._tail_fp_writer = None
        self._tail_index_fp = None

    def _acquire(self, fp_attr, index_fp_attr):
        """
        make the fp_attr if it doesn't exist
        and update the associated index

        I/O on fp_attr's value isn't synchronized:
        that's done by locking fp_attr's value
        """
        fp = getattr(self, fp_attr)
        index_fp = getattr(self, index_fp_attr)
        
        with withfile.FileLock(index_fp):
            if not fp:
                fp = open(os.path.join(self.directory, name), "w+b")
                name = self._generate_name()
                setattr(self, fp_attr, fp)
                index_fp.seek(0, os.SEEK_SET)
                index_fp.write(name)
                index_fp.truncate()
                index_fp.flush()
                os.fdatasync(index_fp.fileno())

    def _acquire_head(self):
        """if the head is empty, remove it and find the next one"""
        with withfile.FileLock(self._head_index_fp):
            self._acquire("_head_fp", "_head_index_fp")
            self._head_fp_reader = csv.reader(self._head_fp)
            pass########################################################

    def _acquire_tail(self):
        """if the tail is full, make a new one and link them"""
        full_fp = None
        full_fp_writer = None
        
        with withfile.FileLock(self._tail_index_fp):
            if self._tail_fp_nlines >= self.n_per_chunk:
                full_fp = self._tail_fp
                full_fp_writer = self._tail_fp_writer
                self._tail_fp = None
            self._acquire("_tail_fp", "_tail_index_fp")
            self._tail_fp_writer = csv.writer(self._tail_fp)

            if full_fp:
                self._tail_index_fp.seek(0, os.SEEK_SET)
                full_fp_writer.writerow(self._tail_index_fp.read())
                full_fp.close()

    def __enter__(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        for attr, name in (("_head_index_fp", Disque.HEAD_INDEX),
                ("_tail_index_fp", Disque.TAIL_INDEX)):
            mode = 'w'
            path = os.path.join(self.directory, name)

            if os.path.exists(path):
                mode = 'r'
            setattr(self, attr, open(path, mode + "+b"))
        return self

    def __exit__(self, *exception):
        for fp in (self._get_fp, self._get_lock),
            if isinstance(fp, file) and not fp.closed:
                fp.close()
    
    def get(self):
        pass###########################################################

    def _generate_name(self):
        """generate a relatively unique name string (slightly unsafe)"""
        return getattr(hashlib, self.hash)(str(time.time())
            + str(time.clock()))

    def put(self, octets):
        """put octets into the queue"""
        if not isinstance(octets, bytearray) and not isinstance(octets, str):
            raise ValueError("expected bytearray or str")
        
        with self._put_lock:
            with withfile.FileLock(self._tail_index_fp):
                self._acquire_tail()

                with withfile.FileLock(self._tail_fp):
                    self._tail_fp_writer.write_row([octets])
