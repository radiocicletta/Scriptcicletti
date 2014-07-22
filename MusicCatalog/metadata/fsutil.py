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
from collections import defaultdict
from mutagen.wavpack import WavPack
from ID3 import ID3, InvalidTagError
import logging
import os
import sys


FS_ENCODING = sys.getfilesystemencoding()
DECODERS = (
    MP3,
    FLAC,
    MP4,
    MonkeysAudio,
    Musepack,
    WavPack,
    TrueAudio,
    OggVorbis,
    OggTheora,
    OggSpeex,
    OggFLAC)


def collect_metadata(
        abspathitem,
        db,
        recentartists,
        recentalbums,
        recentgenres,
        queues,
        condition):
    """ id3 tags retriever """

    id3item = None
    id3v1item = {}
    id3v1 = False
    id3v2 = False
    for decoder in DECODERS:
        try:
            id3item = decoder(abspathitem)
            break
        except Exception as e:
            logging.error(e)
    try:
        id3v1item = defaultdict(lambda: 'unknown', ID3(abspathitem).as_dict())
    except InvalidTagError as e:
        logging.error(e)
    except Exception as e:
        logging.error(e)

    title = id3v1item['TITLE'].strip().lower()
    titleclean = re.sub("[^\w]*", "", title)
    artist = id3v1item['ARTIST'].strip().lower()
    album = id3v1item['ALBUM'].strip().lower()
    albumclean = re.sub("[^\w]*", "", album)
    genre = id3v1item['GENRE'].strip().lower()
    genreclean = re.sub("[^\w]+", "", genre).strip().lower()
    if not id3item:
        logging.warning("No ID3 informations found")
        return
    length = 0.0

    try:
        title = " ".join(id3item['TIT2'].text).strip().lower()
        id3v2 = True
    except Exception as e:
        logging.error(e)
    try:
        titleclean = re.sub("[^\w]*", "", title)
    except Exception as e:
        logging.error(e)
    try:
        artist = " ".join(id3item['TPE1'].text).strip().lower()
        id3v2 = True
    except Exception as e:
        logging.error(e)
    try:
        album = " ".join(id3item['TALB'].text).strip().lower()
        id3v2 = True
    except Exception as e:
        logging.error(e)
    try:
        albumclean = re.sub("[^\w]*", "", album)
    except Exception as e:
        logging.error(e)
    try:
        genre = " ".join(id3item['TCON'].text).strip().lower()
        id3v2 = True
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
    if not id3v2:
        logging.warning("No ID3v2 informations found")
        return

    with condition:
        try:
            ar = artist
            if not artist in recentartists.keys():
                if not db.execute(
                    "select id from artist where name = ?",
                        (ar,)).fetchone():
                    db.execute(
                        "insert into artist(name) values(?)", (ar,))
                    db.commit()
                recentartists[artist] = db.execute(
                    "select id from artist where name = ?",
                    (ar,)).fetchone()[0]
        except Exception as e:
            logging.error(e)

    with condition:
        try:
            al = albumclean
            if not album in recentalbums.keys():
                if not db.execute(
                    "select id from album where titleclean = ?",
                        (al,)).fetchone():
                    db.execute(
                        "insert into album(title, titleclean) "
                        "values(?, ?)", (album, albumclean))
                    db.commit()
                recentalbums[album] = db.execute(
                    "select id from album where titleclean = ?",
                    (al,)).fetchone()[0]
        except Exception as e:
            logging.error(e)

    with condition:
        try:
            ge = genre
            if not genre in recentgenres.keys():
                if not db.execute(
                    "select id from genre where desc = ?",
                        (ge,)).fetchone():
                    db.execute(
                        "insert or ignore into genre(desc, descclean) "
                        "values(?, ?)", (genre, genreclean))
                    db.commit()
                recentgenres[genre] = db.execute(
                    "select id from genre where desc = ?",
                    (ge,)).fetchone()[0]
        except Exception as e:
            logging.error(e)

    with condition:
        try:
            db.execute(
                "insert or replace into song("
                "title, titleclean, artist_id, "
                "genre_id, album_id, path, length) "
                "values (?,?,?,?,?,?,?)",
                (
                    title,
                    titleclean,
                    recentartists[artist],
                    recentgenres[genre],
                    recentalbums[album],
                    abspathitem.decode(FS_ENCODING), length))

            logging.debug("collect_metadata putting new artist on queue")
            for q in queues:
                if not q.full():
                    q.put_nowait((abspathitem, title, artist, album))
                else:
                    q.put((abspathitem, title, artist, album), block=True)
            db.commit()
        except Exception as e:
            logging.error(e)


def breadth_scan(path, db, queues, condition, depth=1):
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
                collect_metadata(
                    abspathitem,
                    db,
                    recentartists,
                    recentalbums,
                    recentgenres,
                    queues,
                    condition)
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
                collect_metadata(
                    abspathitem,
                    db,
                    recentartists,
                    recentalbums,
                    recentgenres,
                    queues,
                    condition)
