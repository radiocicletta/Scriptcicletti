import sys
import os
import sqlite3 as dbapi
import logging
from metadata.fsutil import collect_metadata, breadth_scan
FS_ENCODING = sys.getfilesystemencoding()


class SubtreeListener():

    def __init__(self, dbpath, lf_queue, di_queue, fd_queue, condition):
        self.dbpath = dbpath
        self.recents = []
        self.recentartists = {}
        self.recentalbums = {}
        self.recentgenres = {}
        self.recentsongs = {}
        self.lf_queue = lf_queue
        self.di_queue = di_queue
        self.fd_queue = fd_queue
        self.condition = condition
        self.cookies = {}

    def process(self, abspathitem):

        db = dbapi.connect(self.dbpath)
        if os.path.isdir(abspathitem):
            breadth_scan(abspathitem, db, self.lf_queue, self.di_queue, self.fd_queue, self.condition, True)
        else:
            if abspathitem in self.recents:
                db.close()
                return
            logging.debug('Collecting metadata')
            collect_metadata(abspathitem, db, self.recentartists, self.recentalbums, self.recentgenres, self.lf_queue, self.di_queue, self.fd_queue, self.condition)

            if len(self.recents) >= 20:
                self.recents.pop(0)
            self.recents.append(abspathitem)

            if len(self.recentalbums) > 20:
                self.recentalbums.clear()
            if len(self.recentgenres) > 20:
                self.recentgenres.clear()
            if len(self.recentsongs) > 20:
                self.recentsongs.clear()
            if len(self.recentartists) > 20:
                self.recentartists.clear()
        db.close()

    def get_handler(self):
        """ returns the handler to use """
        return None
