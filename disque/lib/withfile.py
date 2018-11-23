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
import fcntl

__doc__ = "wrappers for use with a file, in an enterable"

class BufferedReader:
    """a buffered reader for a file"""

    def __init__(self, fp, buflen = 1048576):
        self.buflen = buflen
        assert isinstance(fp, file) and not fp.closed, \
            "fp must be an open file"
        self.fp = fp

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        pass

    def __iter__(self):
        return self

    def next(self):
        """return the next 1-buflen bytes from the file"""
        chunk = self.fp.read(self.buflen)

        if not chunk:
            raise StopIteration()
        return chunk

class FileLock:
    """
    an flock-oriented enterable for clarity

    if complain evaluates to True, raise any pertinent errors
    """

    def __init__(self, fp, complain = False):
        self.complain = complain
        self.fp = fp
        self.locked = False

    def __enter__(self):
        try:
            fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX)
            self.locked = True
        except IOError as e:
            if self.complain:
                raise e
        return self

    def __exit__(self, *exception):
        if self.locked:
            try:
                fcntl.flock(self.fp.fileno(), fcntl.LOCK_UN)
            except IOError as e:
                if self.complain:
                    raise e
        elif self.complain:
            raise IOError("already unlocked")
