from metadata.fsutil import breadth_scan
from pyinotify import ProcessEvent, WatchManager, ThreadedNotifier, ExcludeFilter, IN_CLOSE_WRITE, IN_DELETE, IN_MOVED_FROM, IN_MOVED_TO, IN_CREATE, IN_ISDIR
from listener import SubtreeListener
import os
import sys
import sqlite3 as dbapi
import logging
FS_ENCODING = sys.getfilesystemencoding()


class InotifyThread(ThreadedNotifier):

    def __init__(self, listener):
        self.wm_auto = WatchManager()
        ThreadedNotifier.__init__(self, self.wm_auto, listener)

    def observe(self, path, subtreemask=IN_CLOSE_WRITE | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO | IN_CREATE | IN_ISDIR, excludelist=[]):
        excludefilter = ExcludeFilter(excludelist)
        self.wdd_sb = self.wm_auto.add_watch(path, subtreemask, auto_add=True, rec=True, exclude_filter=excludefilter)


class InotifySubtreeListener(SubtreeListener, ProcessEvent):
    """ handler for inotify thread events """

    def __init__(self, dbpath, lf_queue, di_queue, fd_queue, condition):
        SubtreeListener.__init__(self, dbpath, lf_queue, di_queue, fd_queue, condition)
        ProcessEvent.__init__(self)

    def process_IN_CLOSE_WRITE(self, evt):
        if evt.name[0] == '.':
            return
        logging.debug("IN_CLOSE_WRITE %s" % evt.path)
        self.process_event(evt)

    def process_IN_MOVED_FROM(self, evt):
        if evt.name[0] == '.':
            return
        logging.debug("IN_MOVED_FROM %s" % evt.path)
        self.process_IN_DELETE(evt)

    def process_IN_MOVED_TO(self, evt):
        if evt.name[0] == '.':
            return
        logging.debug("IN_MOVED_TO %s" % evt.path)
        newpath = "%s/%s" % (evt.path, evt.name)
        if os.path.isdir(newpath):
            self.process_IN_ISDIR(evt)
        else:
            self.process_event(evt)

    def process_IN_ISDIR(self, evt):
        if evt.name[0] == '.':
            return
        logging.debug("IN_ISDIR %s" % evt.path)
        newpath = "%s/%s" % (evt.path, evt.name)
        db = dbapi.connect(self.dbpath)
        #exists = db.execute("select id from song where path like ? limit 1;", ("%s%%" % evt.path,)).fetchone()
        #if not exists:
        breadth_scan(newpath, db, self.lf_queue, self.di_queue, self.fd_queue, self.condition, True)
        db.close()

    def process_IN_DELETE(self, evt):
        if evt.name[0] == '.':
            return
        logging.debug("IN_DELETE %s" % evt.path)
        abspathitem = "%s/%s" % (evt.path, evt.name)
        if abspathitem in (self.dbpath, "%s-journal" % self.dbpath):
            return

        self.condition.acquire()
        db = dbapi.connect(self.dbpath)
        try:
            if evt.dir:  # os.path.isdir(abspathitem):
                songs = db.execute("select id from song where path like ?;", ("%s%%" % abspathitem.decode(FS_ENCODING),))
                song_id = songs.fetchall()
                self.recents = []
            else:
                songs = db.execute("select id from song where path = ?;", (abspathitem.decode(FS_ENCODING),))
                song_id = [songs.fetchone()]
            if song_id and song_id[0]:
                for s_i in song_id:
                    db.execute("delete from song where id = ?;", s_i)
                    db.execute("delete from song_x_tag where song_id = ?;", s_i)
            db.commit()
        except Exception as e:
            logging.error(e)
        finally:
            db.close()
            self.condition.release()
        if abspathitem in self.recents:
            self.recents.remove(abspathitem)

    def process_event(self, evt):
        if evt.pathname == self.dbpath:
            return
        self.process("%s/%s" % (evt.path, evt.name))

    def get_handler(self):
        return self
