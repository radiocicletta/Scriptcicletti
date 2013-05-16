from metadata.fsutil import breadth_scan
from listener import SubtreeListener
from fsevents import Observer, Stream, IN_CREATE, IN_DELETE, IN_MODIFY, IN_MOVED_FROM, IN_MOVED_TO, IN_ATTRIB
from select import kqueue, kevent
import sqlite3 as dbapi
import os
import sys
import select
import logging
import re
FS_ENCODING = sys.getfilesystemencoding()


class FseventsThread(Observer):

    def __init__(self, listener):
        self.listener = listener
        Observer.__init__(self)

    def observe(self, path, excludelist=[]):
        stream = Stream(self.listener, path, file_events=True)
        self.exclude = excludelist
        self.schedule(stream)


class FseventsSubtreeListener(SubtreeListener):
    """ handler for inotify thread events """
    # IN_CREATE, IN_DELETE, IN_MODIFY, IN_MOVED_FROM, IN_MOVED_TO

    def __init__(self, dbpath, lf_queue, di_queue, fd_queue, condition):
        SubtreeListener.__init__(self, dbpath, lf_queue, di_queue, fd_queue, condition)

    def process_event(self, evt):
        if evt.name == self.dbpath or os.path.basename(evt.name)[0] == '.':
            return
        elif evt.mask == IN_CREATE:
            logging.debug("IN_CREATE %s" % evt.name)
            fd = os.open(evt.name, os.O_RDONLY)
            events = [kevent(fd, filter=select.KQ_FILTER_READ, flags=select.KQ_EV_ADD | select.KQ_EV_CLEAR, fflags=select.KQ_NOTE_EXTEND)]
            kq = kqueue()
            size = -1
            while True:
                evts = kq.control(events, 10, 5)
                newsize = os.stat(evt.name).st_size
                if newsize == size and size != 0:
                    break
                size = newsize
            logging.debug("SIZE %d" % os.stat(evt.name).st_size)
            os.close(fd)
            self.process(evt.name)

        elif evt.mask == IN_MODIFY:
            logging.debug("IN_MODIFY %s" % evt.name)
            self.process(evt.name)

        # elif evt.mask == IN_ATTRIB:
        #     logging.debug("IN_ATTRIB %s" % evt.name)
        #     self.process(evt.name)

        elif evt.mask == IN_MOVED_TO:
            logging.debug("IN_MOVED_TO %s" % evt.name)
            if os.path.isdir(evt.name):
                db = dbapi.connect(self.dbpath)
                breadth_scan(evt.name, db, self.lf_queue, self.di_queue, self.fd_queue, self.condition, True)
                db.close()
            else:
                self.process(evt.name)

        elif evt.mask in (IN_DELETE, IN_MOVED_FROM):
            logging.debug("IN_DELETE %s" % evt.name)
            if evt.name in (self.dbpath, "%s-journal" % self.dbpath):
                return

            self.condition.acquire()
            db = dbapi.connect(self.dbpath)
            try:
                songs = db.execute("select id from song where path = ?;", (evt.name.decode(FS_ENCODING),))
                song_id = [songs.fetchone()]
                if not song_id[0]:
                    logging.debug("DELETION DIRECTORY (?): %s" % evt.name)
                    songs = db.execute("select id from song where path like ?;", ("%s%%" % evt.name.decode(FS_ENCODING),))
                    song_id = songs.fetchall()
                    self.recents = []
                if song_id and song_id[0]:
                    logging.debug("SONG_ID: %s" % song_id)
                    for s_i in song_id:
                        db.execute("delete from song where id = ?;", s_i)
                        db.execute("delete from song_x_tag where song_id = ?;", s_i)
                    db.commit()
            except Exception as e:
                logging.error(e)
            finally:
                db.close()
                self.condition.release()
            if evt.name in self.recents:
                self.recents.remove(evt.name)
        else:
            logging.debug("IN_??? %s" % evt.mask)

    def get_handler(self):
        def callback(evt):
            self.process_event(evt)

        return callback
