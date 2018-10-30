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
import json
import os
import Queue
import sys
import threading
import time

sys.path.append(os.path.realpath(__file__))

import withfile

__doc__ = "large-scale, disk-based queues"

class DiskQueue:
    """
    a parallel-safe, on-disk queue (formatted as a chunked linked list)

    general structure is as follows:
        nodes form the linked list, but are stored separately from data chunks
        the head node is stored at "directory/head"
        in the absence of a head node, the queue is perceived to be empty

        writing occurs directly into the tail node

        reading is chunked (ie. 1 chunk is loaded every 1-max_chunk_size calls)
    """

    def __init__(self, directory = os.getcwd(), max_chunk_size = 512):
        self.directory = os.path.realpath(directory)
        self.head_path = os.path.join(self.directory, "head")
        assert isinstance(max_chunk_size, int) and max_chunk_size > 0, \
            "max_chunk_size must be an integer > 0"
        self.max_chunk_size = max_chunk_size
        self._output_chunk = Queue.Queue()

    def empty(self):
        return self._chunk.empty() and self.head == None

    def __enter__(self):
        pass

    def __exit__(self, *exception):
        pass

    def get(self):
        pass

    def __len__(self):
        """calculate the queue size (O(N chunks))"""
        pass

    def put(self, value):
        pass

class DiskQueueNodeIO:
    """an on-disk linked list queue node"""

    def __init__(self, fp, data = None, next = None):
        self.data = data
        assert isinstance(fp, file) and not fp.closed, \
            "fp must be an open file"
        self._fp = fp
        self.next = next

    def read(self):
        """read the node from the file into the current instance"""
        mapping = json.load(self._fp)
        self.data = mapping["data"]
        self.next = mapping["next"]

    def write(self):
        """write the node instance into the file"""
        json.dump(self._fp, {"data": self.data, "node": self.node})

class DiskQueueChunkIO:
    """a chunk of queue data stored as a CSV list"""
    
    def __init__(self, fp):
        assert isinstance(fp, file) and not fp.closed, \
            "fp must be an open file"
        self._fp = fp

    def read(self):
        pass

    def remove(self):
        os.remove(self.path)

    def write(self, data):
        pass
