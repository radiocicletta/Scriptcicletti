from mutagen.flac import FLAC
import re
from mutagen.mp3 import MP3, HeaderNotFoundError
from mutagen.monkeysaudio import MonkeysAudio
from mutagen.mp4 import MP4
from mutagen.musepack import Musepack
from mutagen.oggflac import OggFLAC
from mutagen.oggspeex import OggSpeex
from mutagen.oggtheora import OggTheora
from mutagen.oggvorbis import OggVorbis
from mutagen.trueaudio import TrueAudio
from mutagen.wavpack import WavPack
from ID3 import ID3
import logging
import os
import sys
import sqlite3 as dbapi
try:  # TODO: add FSEvents support
    from pyinotify import WatchManager, ThreadedNotifier, ProcessEvent, ExcludeFilter, IN_CREATE, IN_MOVED_TO, IN_CLOSE_WRITE, IN_DELETE, IN_ISDIR, IN_MOVED_FROM
except:
    pass
DECODERS = (MP3, FLAC, MP4, MonkeysAudio, Musepack, WavPack, TrueAudio, OggVorbis, OggTheora, OggSpeex, OggFLAC)
FS_ENCODING = sys.getfilesystemencoding()


class SubtreeListener(ProcessEvent):
    """ handler for inotify thread events """

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
                song_id = songs.fetchone()
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
        db = dbapi.connect(self.dbpath)
        abspathitem = "%s/%s" % (evt.path, evt.name)
        if os.path.isdir(abspathitem):
            breadth_scan(abspathitem, db, self.lf_queue, self.di_queue, self.fd_queue, self.condition, True)
        else:
            if abspathitem in self.recents:
                db.close()
                return
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


def create_subtreelistener(path, dbpath, lf_queue, di_queue, fd_queue, condition):
    """ create a subtree listener """
    wm_auto = WatchManager()
    subtreemask = IN_CLOSE_WRITE | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO | IN_CREATE | IN_ISDIR
    excludefilter = ExcludeFilter(["(.*)sqlite"])
    thread = ThreadedNotifier(wm_auto, SubtreeListener(dbpath, lf_queue, di_queue, fd_queue, condition))
    thread.start()

    wdd_sb = wm_auto.add_watch(path, subtreemask, auto_add=True, rec=True, exclude_filter=excludefilter)
    return thread, wdd_sb


def collect_metadata(abspathitem, db, recentartists, recentalbums, recentgenres, lf_queue, di_queue, fd_queue, condition):
    """ id3 tags retriever """

    id3item = None
    id3v1item = {'TITLE': '', 'ARTIST': '', 'ALBUM': '', 'GENRE': ''}
    for decoder in DECODERS:
        try:
            id3item = decoder(abspathitem)
            id3v1item = ID3(abspathitem).as_dict()
            break
        except Exception as e:
            logging.error(e)
    if not id3item:
        logging.warning("No ID3v2 informations found")
        return
    if 'TITLE' in id3v1item:
        title = id3v1item['TITLE'].strip().lower()
        titleclean = re.sub("[^\w]*", "", title)
    else:
        title = "unknown"
        titleclean = "unknown"
    if 'ARTIST' in id3v1item:
        artist = id3v1item['ARTIST'].strip().lower()
    else:
        artist = "unknown"
    if 'ALBUM' in id3v1item:
        album = id3v1item['ALBUM'].strip().lower()
        albumclean = re.sub("[^\w]*", "", album)
    else:
        album = "unknown"
        albumclean = "unknown"
    if 'GENRE' in id3v1item:
        genre = id3v1item['GENRE'].strip().lower()
        genreclean = re.sub("[^\w]+", "", genre).strip().lower()
    else:
        genre = "unknown"
        genreclean = "unknown"
    length = 0.0

    try:
        title = " ".join(id3item['TIT2'].text).strip().lower()
    except Exception as e:
        logging.error(e)
    try:
        titleclean = re.sub("[^\w]*", "", title)
    except Exception as e:
        logging.error(e)
    try:
        artist = " ".join(id3item['TPE1'].text).strip().lower()
    except Exception as e:
        logging.error(e)
    try:
        album = " ".join(id3item['TALB'].text).strip().lower()
    except Exception as e:
        logging.error(e)
    try:
        albumclean = re.sub("[^\w]*", "", album)
    except Exception as e:
        logging.error(e)
    try:
        genre = " ".join(id3item['TCON'].text).strip().lower()
    except Exception as e:
        logging.error(e)
    try:
        genreclean = re.sub("[^\w]+", "", genre).strip().lower()
    except Exception as e:
        logging.error(e)
    try:
        length = float(id3item['TLEN'])
    except Exception as e:
        logging.error(e)

    condition.acquire()
    try:
        ar = artist if artist else 'unknown'
        if not artist in recentartists.keys():
            if not db.execute("select id from artist where name = ?", (ar,)).fetchone():
                db.execute("insert into artist(name) values(?)", (ar,))
                db.commit()
            recentartists[artist] = db.execute("select id from artist where name = ?", (ar,)).fetchone()[0]
    except Exception as e:
        logging.error(e)
    finally:
        condition.release()

    condition.acquire()
    try:
        al = albumclean if albumclean else 'unknown'
        if not album in recentalbums.keys():
            if not db.execute("select id from album where titleclean = ?", (al,)).fetchone():
                db.execute("insert into album(title, titleclean) values(?, ?)", (album, albumclean))
                db.commit()
            recentalbums[album] = db.execute("select id from album where titleclean = ?", (al,)).fetchone()[0]
    except Exception as e:
        logging.error(e)
    finally:
        condition.release()

    condition.acquire()
    try:
        ge = genre if genre else 'unknown'
        if not genre in recentgenres.keys():
            if not db.execute("select id from genre where desc = ?", (ge,)).fetchone():
                db.execute("insert into genre(desc, descclean) values(?, ?)", (genre, genreclean))
                db.commit()
            recentgenres[genre] = db.execute("select id from genre where desc = ?", (ge,)).fetchone()[0]
    except Exception as e:
        logging.error(e)
    finally:
        condition.release()

    condition.acquire()
    try:
        db.execute("insert or replace into song(title, titleclean, artist_id, genre_id, album_id, path, length) values (?,?,?,?,?,?,?)", (title, titleclean, recentartists[artist], recentgenres[genre], recentalbums[album], abspathitem.decode(FS_ENCODING), length))

        logging.debug("collect_metadata putting new artist on queue")
        if not lf_queue.full():
            lf_queue.put_nowait((abspathitem, title, artist))
        else:
            lf_queue.put((abspathitem, title, artist), block=True)
        if not di_queue.full():
            di_queue.put_nowait((abspathitem, title, artist, album))
        else:
            di_queue.put((abspathitem, title, artist, album), block=True)
        if not fd_queue.full():
            fd_queue.put_nowait((abspathitem, title, artist))
        else:
            fd_queue.put((abspathitem, title, artist), block=True)

    except Exception as e:
        logging.error(e)
    finally:
        db.commit()
        condition.release()


def breadth_scan(path, db, lf_queue, di_queue, fd_queue, condition, depth=1):
    """ Breadth scan a subtree """

    scanpath = [path, ]

    while len(scanpath):
        curdir = scanpath.pop()
        recentartists = {}
        recentsong = {}
        recentalbums = {}
        recentgenres = {}

        logging.debug(os.listdir(curdir))
        for item in os.listdir(curdir):
            abspathitem = "%s/%s" % (curdir, item)
            logging.debug("Collecting informations on %s" % item)
            if os.path.isdir(abspathitem) and depth:
                scanpath.append(abspathitem)
            else:
                collect_metadata(abspathitem, db, recentartists, recentalbums, recentgenres, lf_queue, di_queue, fd_queue, condition)
    """ Breadth scan a subtree """

    scanpath = [path, ]

    while len(scanpath):
        curdir = scanpath.pop()
        recentartists = {}
        recentsong = {}
        recentalbums = {}
        recentgenres = {}

        logging.debug(os.listdir(curdir))
        for item in os.listdir(curdir):
            abspathitem = "%s/%s" % (curdir, item)
            logging.debug("Collecting informations on %s" % item)
            if os.path.isdir(abspathitem) and depth:
                scanpath.append(abspathitem)
            else:
                collect_metadata(abspathitem, db, recentartists, recentalbums, recentgenres, lf_queue, di_queue, fd_queue, condition)
