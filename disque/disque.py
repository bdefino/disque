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
import collections
import csv
import hashlib
import json
import os
import Queue
import sys
import thread
import time

from lib import withfile

__doc__ = "persisent queueing"

class Disque:
    """
    a disk-based FIFO structure intended to alleviate memory-hungry queuing

    access to the disque is controlled via flock calls on the index file,
    which stores pertinent information in JSON format

    entries are chunked on-disk using CSV format,
    followed by the the name of the next chunk;
    because of this, I/O occurs one chunk at a time
    which may result in a slightly different order in which values
    are obtained across multiple processes

    because this operates by buffering entries,
    not everything may be on disk at a time
    """

    CHUNK_SIZE = "chunk size"
    HEAD = "head"
    INDEX = ".index"
    NEXT_TAIL = "next tail"
    
    def __init__(self, directory = os.getcwd(), hash = "sha256",
            chunk_size = 512):
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.directory = directory
        self._get_lock = thread.allocate_lock()
        self.hash = hash
        self._index = {Disque.CHUNK_SIZE: chunk_size, Disque.HEAD: "",
            Disque.NEXT_TAIL: ""} # current index values
        self._index_fp = self._persistent_open(os.path.join(self.directory,
            Disque.INDEX))
        self._inbuf = collections.deque()
        self._outbuf = collections.deque()
        self._put_lock = thread.allocate_lock()

        self._load_index()

        if self._index[Disque.CHUNK_SIZE] <= 0:
            raise ValueError("chunk_size must be greater than 0")

    def _dump_index(self):
        """dump the index"""
        with withfile.FileLock(self._index_fp):
            self._index_fp.seek(0, os.SEEK_SET)
            json.dump(self._index, self._index_fp)
            self._fsync(self._index_fp)

    def _fsync(self, fp):
        """flush a file-like objects buffer, synching to disk if possible"""
        fp.flush()

        if hasattr(fp, "fileno"):
            os.fdatasync(getattr(fp, "fileno")())

    def _generate_name(self):
        """generate a relatively unique, path-safe string"""
        id = ' '.join((str(e) for e in (time.clock(), time.time(),
            os.urandom(128))))
        return getattr(hashlib, self.hash)(id).hexdigest()

    def get(self):
        """get octets from the disque"""
        with self._get_lock:
            if not len(self._outbuf): # read the head chunk
                with withfile.FileLock(self._index_fp):
                    self._load_index()

                    if not self._index[Disque.HEAD]: # no head set, use tail
                        if not self._index[Disque.TAIL]:
                            self._write_outbuf(True) # flush to new tail
                        self._index[Disque.HEAD] = self._index[Disque.TAIL]
                    path = os.path.join(self.directory,
                        self._index[Disque.HEAD])
                    
                    if not os.path.exists(path):
                        raise ValueError("empty")
                    
                    with open(path, "rb") as fp:
                        fp_reader = csv.reader(fp)

                        for row in fp_reader:
                            self._outbuf.append(row[0])

                        if len(self._outbuf): # read link
                            self._index[Disque.HEAD] = self._outbuf.pop()
                    os.remove(path)
                    self._dump_index()

            if not len(self._outbuf):
                raise ValueError("empty")
            return self._outbuf.popleft()

    def _load_index(self, re_sync = True):
        """load the index, then optionally re-sync to ensure valid data"""
        with withfile.FileLock(self._index_fp):
            self._index_fp.seek(0, os.SEEK_SET)
            index = None

            try:
                index = json.load(self._index_fp)
            except ValueError:
                pass

            if isinstance(index, dict):
                index = {str(k): v for k, v in index.iteritems()}
                
                for key, type in ((Disque.CHUNK_SIZE, int), (Disque.HEAD, str),
                        (Disque.NEXT_TAIL, str)): # caste
                    if key in index:
                        try:
                            self._index[key] = type(self._index[key])
                        except (TypeError, ValueError):
                            pass

            if re_sync: # in case the index wasn't valid
                self._dump_index()

    def _persistent_open(self, path):
        """open a path using a mode that'll preserve its contents"""
        return open(path, ('r' if os.path.exists(path) else 'w') + "+b")

    def put(self, octets, flush = False):
        """put octets into the disque, optionally flushing the buffer"""
        if not isinstance(octets, bytearray) and not isinstance(octets, str) \
                and not isinstance(octets, unicode):
            raise TypeError("octets must be a bytearray, str," \
                " or unicode instance")

        with self._put_lock:
            self._inbuf.append(octets)

            with withfile.FileLock(self._index_fp):
                self._write_outbuf(flush)
    
    def sync(self):
        """
        flush the buffers into the disque
        
        this may result in a single small chunk joining the head
        and the existing output buffer
        """
        with withfile.FileLock(self._index_fp):
            current = self._generate_name()
            next = self._generate_name()
            
            while len(self._outbuf): # re-insert the buffered head(s)
                with self._persistent_open(os.path.join(self.directory,
                        current)) as fp:
                    fp_writer = csv.writer(fp)
                    i = 0

                    while i < self._index[Disque.CHUNK_SIZE] \
                            and len(self._outbuf):
                        fp_writer.writerow([self._outbuf.popleft()])
                        i += 1

                    if i == self._index[Disque.CHUNK_SIZE]:
                        if not len(self._outbuf) \
                                and self._index[Disque.HEAD]: # link to head
                            next = self._index[Disque.HEAD]
                        else: # update the index
                            self._index[Disque.TAIL] = next
                    fp_writer.writerow([next]) # link
                    self._fsync(fp)
                current = next
                next = self._generate_name()
            self._dump_index()
            self._write_outbuf(True) # flush the buffered tail(s)

    def _write_outbuf(self, flush = False):
        """write a chunk (or all chunks, if flush is specified) from _inbuf"""
        if not len(self._inbuf) >= self._index[Disque.CHUNK_SIZE] \
                and not flush:
            return
        
        with withfile.FileLock(self._index_fp):
            self._load_index()

            if not self._index[Disque.NEXT_TAIL]: # seed
                self._index[Disque.NEXT_TAIL] = self._generate_name()
            
            if not self._index[Disque.HEAD]: # redirect
                self._index[Disque.HEAD] = self._index[Disque.NEXT_TAIL]
            tail = self._index.pop(Disque.NEXT_TAIL)
            self._index[Disque.NEXT_TAIL] = self._generate_name() # next link
            
            with self._persistent_open(os.path.join(self.directory, tail)) \
                    as fp:
                i = 0
                fp_writer = csv.writer(fp)

                while i < self._index[Disque.CHUNK_SIZE]  and len(self._inbuf):
                    fp_writer.writerow([self._inbuf.popleft()])
                    i += 1
                fp_writer.writerow([self._index[Disque.NEXT_TAIL]]) # link
                self._fsync(fp)
            self._dump_index()

            if flush and len(self._inbuf): # flush the remainder
                self._write_outbuf(True)
