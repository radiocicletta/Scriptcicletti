from parameters import * # a file parameters.py containing last.fm variables USERNAME and APIKEY
import threading
import re
import pylast
from urllib import unquote, quote
from Queue import Empty
from httplib import HTTPConnection
from itertools import permutations
import subprocess
import sqlite3 as dbapi
import logging
import sys
# import musicbrainzgs
from time import time
from utils import hamming, levenshtein, which

try:
    import simplejson as json
except:
    import json

FS_ENCODING = sys.getfilesystemencoding()


class FiledataThread(threading.Thread):
    """ slow file analyzer thread.
        Retrieve song's length and bpm where availble """

    daemon = True

    def __init__(self, queue, condition, dbpath):
        threading.Thread.__init__(self, name="FiledataThread")
        self.queue = queue
        self.condition = condition
        self.dbpath = dbpath
        self.running = True
        self.soxi = which('soxi')
        self.sox = which('sox')
        self.soundstretch = which('soundstretch')

    def stop(self):
        self.running = False

    def run(self):

        self.db = dbapi.connect(self.dbpath)
        logging.debug("sox: %s, soxi: %s, soundstretch: %s" % (self.sox, self.soxi, self.soundstretch))
        while self.running:
            try:
                logging.debug("getting queue...")
                path, title, artist, album = self.queue.get()
            except Empty as e:
                logging.warning(e)
                continue
            except Exception as e:
                logging.error("%s." % e)
                continue
            if not path:
                continue
            for i in ('30', '60', '90', '120'):
                bpm_pattern = None
                soxi_output = 0
                try:
                    logging.info("Analyzing %s file" % path)
                    if self.soxi:
                        logging.debug("soxi_process for %s" % path)
                        soxi_process = subprocess.Popen(
                            [self.soxi, "-D", path],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
                        output = soxi_process.communicate()
                        soxi_output = float(output[0])
                    if self.sox and self.soundstretch:
                        logging.debug("sox_process for %s" % path)
                        sox_process = subprocess.Popen(
                            [self.sox, path, "-t", "wav",
                             "/tmp/.stretch.wav", "trim", "0", i],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)  # well done, dear sox friend. Well done.
                        # sox_process.wait()
                        sox_process.communicate()
                        logging.debug("bpm_process for %s" % path)
                        bpm_process = subprocess.Popen(
                            [self.soundstretch, "/tmp/.stretch.wav",
                             "-bpm", "-quick", "-naa"],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
                        bpm_output = bpm_process.communicate()
                        bpm_pattern = re.search("Detected BPM rate ([0-9]+)",
                                                bpm_output[1], re.M)
                except Exception as e:
                    logging.error(e)
                bpm = bpm_pattern and float(bpm_pattern.groups()[0]) or 0.0
                logging.info("Detected bpm %s with %s seconds sampling" % (bpm, i))
                if bpm:
                    break

            with self.condition:
                try:
                    self.db.execute("update song set bpm = ?, length = ? where path = ?",
                                    (bpm, soxi_output, path.decode(FS_ENCODING)))
                    self.db.commit()
                except:
                    pass
        self.db.close()


class LastFMMetadataThread(threading.Thread):
    """ last.fm client Thread. Collect top tags for each song """

    daemon = True

    def __init__(self, queue, condition, dbpath):
        threading.Thread.__init__(self, name="LastFMMetadataThread")
        self.daemon = True
        self.queue = queue
        self.condition = condition
        self.dbpath = dbpath
        self.lastfm = pylast.LastFMNetwork(username=USERNAME)
        self.lastfm.api_key = APIKEY
        self.running = True

    def stop(self):
        self.running = False

    def run(self):

        self.db = dbapi.connect(self.dbpath)
        while self.running:
            try:
                path, title, artist, album = self.queue.get()
            except Empty as e:
                logging.warning(e)
                continue
            except Exception as e:
                logging.error(e)
                continue
            if not path:
                logging.warning("No path provided")
                continue

            logging.info("Getting tags for %s %s" % (artist, title))
            try:
                tags = self.lastfm.get_track(artist, title).get_top_tags()
            except Exception as e:
                logging.error(e)
                tags = []

            logging.debug("%s tags found" % len(tags))

            if not tags:
                continue

            with self.condition:
                try:
                    song_id = self.db.execute("select id from song where path = ?",
                                              (path.decode(FS_ENCODING), )
                                              ).fetchone()[0]
                    known_tags = self.db.execute("select distinct weight, name, nameclean from song_x_tag left join tag on (tag_id = id) where song_id = ?;", (song_id,)).fetchall()

                    for t in tags:
                        alreadytag = False
                        for kt in known_tags:
                            if t.item.name == kt[1] and t.weight != kt[0]:
                                self.db.execute("update song_x_tag set weight = ? where song_id = ? and tag_id = ?;", (t.weight, song_id, t.item.name))
                                alreadytag = True
                        if not alreadytag:
                            if re.match("^([a-zA-Z0-9] )+[a-zA-Z0-9]$",
                                        t.item.name):  # you damned "e l e c t r o n i c" tag.
                                cleantag = [re.sub("[^a-zA-Z0-9]+", "",
                                                   t.item.name)]
                            else:
                                cleantag = re.sub("[^a-zA-Z0-9 ]+", "",
                                                  t.item.name).strip().lower().split()

                            savedcleantag = []
                            savedcleantag.extend(cleantag)
                            cleantag = filter(lambda x: len(x) > 3, cleantag)

                            if len(cleantag) > 6:
                                cleantag = [" ".join(savedcleantag)]  # as a single phrase

                            tagcomb = permutations(cleantag, len(cleantag))
                            nameclean = "".join(cleantag)
                            for tc in tagcomb:
                                similartag = self.db.execute("select nameclean from tag where name like ?;", ("%%%s%%" % "%".join(tc),)).fetchone()
                                if similartag and len(similartag[0]) == len(nameclean):
                                    nameclean = similartag[0]
                            self.db.execute("insert or ignore into tag (name, nameclean) values (?, ?);", (t.item.name, nameclean))
                            self.db.commit()
                            tagid = self.db.execute("select last_insert_rowid()").fetchone()[0]
                            # tagid = self.db.execute("select id from tag where name = ? ", (t.item.name, )).fetchone()[0]
                            self.db.execute("insert into song_x_tag (song_id, tag_id, weight) values (?, ?, ?);", (song_id, tagid, t.weight))
                    self.db.commit()
                except Exception as e:
                    logging.error(e)
        self.db.close()


class DiscogsMetadataThread(threading.Thread):
    """ Discogs.com client Thread. Search for informations for releases """

    daemon = True

    def __init__(self, queue, condition, dbpath):
        threading.Thread.__init__(self, name="DiscogsMetadataThread")
        self.daemon = True
        self.queue = queue
        self.condition = condition
        self.dbpath = dbpath
        self.running = True

    def stop(self):
        self.running = False

    def run(self):

        self.db = dbapi.connect(self.dbpath)
        # lastrelease = ""
        lastdata = []
        lastquery = ""
        laststatus = 0
        while self.running:
            try:
                path, title, artist, album = self.queue.get()
            except Empty as e:
                logging.warning(e)
                continue
            except Exception as e:
                logging.error(e)
                continue
            if not path or not album:
                logging.warning("No path/album name provided")
                continue

            logging.info("Getting infos for %s %s" % (artist, album))
            try:
                query = u"/database/search?q=%s&type=release" % (quote(" ".join([artist, album])))
                logging.info(query)
                if query == lastquery and laststatus == 200:
                    logging.info("Same request already occurred - skipping")

                conn = HTTPConnection("api.discogs.com", 80)
                conn.request("GET", query)
                response = conn.getresponse()
                if response.status == 200:
                    lastquery = query
                    laststatus = 200
                    results = json.loads(response.read())
                    logging.debug(results)
                    lastdata = results["results"]
                    if len(lastdata):
                        logging.debug("%s results found" % len(lastdata))
                        genres = lastdata[0]["genre"]
                        tags = lastdata[0]["style"]
                    else:
                        tags = []
                        genres = []
            except Exception as e:
                logging.error(e)
                tags = []
                genres = []

            if not tags and not genres:
                continue

            logging.debug("%s tags, %s genres found" % (len(tags), len(genres)))

            with self.condition:
                try:
                    album_id = self.db.execute("select album_id from song where path = ?", (path.decode(FS_ENCODING), )).fetchone()[0]
                    known_tags = self.db.execute("select distinct a.weight, g.desc, g.descclean from album_x_genre a left join genre g on (a.genre_id = g.id) where album_id = ?;", (album_id,)).fetchall()

                    tagset = set(tags + genres)
                    fixedweight = 1 / len(tagset) * 100

                    for t in tagset:  # quite the same as LastFMMetadataThread, except here we use a set
                        logging.debug("analyzing genre %s" % t)
                        alreadytag = False
                        for kt in known_tags:
                            if t == kt[1]:
                                alreadytag = True
                                if fixedweight < kt[0]:
                                    self.db.execute("update album_x_genre set weight = ? where album_id = ? and genre_id = ?;", (fixedweight, album_id, t))
                        if not alreadytag:
                            if re.match("^([a-zA-Z0-9] )+[a-zA-Z0-9]$", t):
                                cleantag = [re.sub("[^a-zA-Z0-9]+", "", t)]
                            else:
                                cleantag = re.sub("[^a-zA-Z0-9 ]+", "", t).lower().split()

                            savedcleantag = []
                            savedcleantag.extend(cleantag)
                            cleantag = filter(lambda x: len(x) > 3, cleantag)

                            if len(cleantag) > 6:
                                cleantag = [" ".join(savedcleantag)]  # as a single phrase

                            tagcomb = permutations(cleantag, len(cleantag))
                            nameclean = "".join(cleantag)
                            for tc in tagcomb:
                                similartag = self.db.execute("select descclean from genre where desc like ?;", ("%%%s%%" % "%".join(tc),)).fetchone()
                                if similartag and len(similartag[0]) == len(nameclean):
                                    nameclean = similartag[0]
                            self.db.execute("insert or ignore into genre (desc, descclean) values (?, ?);", (t, nameclean))
                            self.db.commit()
                            genreid = self.db.execute("select last_insert_rowid()").fetchone()[0]
                            # genreid = self.db.execute("select id from genre where desc = ? ", (t, )).fetchone()[0]
                            logging.debug("create new association for genre %s on album %s" % (t, album_id))
                            self.db.execute("insert into album_x_genre (album_id, genre_id, weight) values (?, ?, ?);", (album_id, genreid, fixedweight))
                    self.db.commit()
                except Exception as e:
                    logging.error(e)
        self.db.close()


class AcoustidMetadataThread(threading.Thread):
    """ Acoustid client Thread. Search for informations for releases """

    daemon = True

    def __init__(self, queue, condition, dbpath):
        threading.Thread.__init__(self, name="AcoustidMetadataThread")
        # musicbrainz.set_useragent('Cycle', '0.1')
        self.daemon = True
        self.queue = queue
        self.condition = condition
        self.dbpath = dbpath
        self.running = True
        self.fpcalc = which('/usr/bin/fpcalc')

    def stop(self):
        self.running = False

    def run(self):

        if not self.fpcalc:
            return

        logging.debug("fpcalc: %s" % self.fpcalc)

        self.db = dbapi.connect(self.dbpath)
        # lastrelease = ""
        lastdata = []
        lastquery = ""
        laststatus = 0
        starttime = time()
        stoptime = starttime + 1
        requests = 0
        while self.running:
            try:
                path, title, artist, album = self.queue.get()
            except Empty as e:
                logging.warning(e)
                continue
            except Exception as e:
                logging.error(e)
                continue
            if not path or not album:
                logging.warning("No path/album name provided")
                continue

            if requests / (stoptime - starttime) > 3:
                sleep(1)
                starttime = stoptime

            logging.info("Getting infos for %s %s" % (artist, album))
            fingerprint = ''
            duration = 0
            try:
                logging.info("Analyzing %s file" % path)
                if self.fpcalc:
                    logging.debug("fingerprint for %s" % path)
                    fpcalc_process = subprocess.Popen(["/usr/bin/fpcalc", path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    fpcalc_output = fpcalc_process.communicate()[0].split('\n')

                    duration = fpcalc_output[1][9:]
                    fingerprint = fpcalc_output[2][12:]
            except Exception as e:
                logging.error(e)

            if fingerprint:
                query = u"/v2/lookup?client=8XaBELgH&meta=recording+releasegroups+tracks+puids+usermeta+compress&duration=%s&format=json&fingerprint=%s" % (duration, fingerprint)
                if query == lastquery and laststatus == 200:
                    logging.info("Same request already occurred - skipping")

                try:
                    conn = HTTPConnection("api.acoustid.org", 80)
                    conn.request("GET", query)
                    response = conn.getresponse()
                except:
                    continue
                puid = ""
                mb_title = ""
                mb_artists = ""
                if response.status != 200:
                    continue
                try:
                    lastquery = query
                    laststatus = 200
                    results = json.loads(response.read())
                    lastdata = results["results"][0]
                    logging.debug(lastdata)
                    release = "releasegroups" in lastdata and len(lastdata) and lastdata["releasegroups"][0]
                    recording = "recordings" in lastdata and len(lastdata) and lastdata["recordings"][0]
                    score = "score" in lastdata and lastdata['score']
                    logging.debug(release)
                    logging.debug(recording)
                    if len(lastdata):
                        logging.debug("%s results found" % len(lastdata))
                        puid = 'puids' in lastdata and lastdata["puids"][0]
                        mbid = release and release['id'] or recording and recording[0]['releasegroups'][0]['id']
                        mb_title = release and release["title"] or recording and recording[0]['title']
                        mb_artists = " ".join([i['name'] for i in (release and release["artists"] or recording and recording[0]['artists'])])
                    logging.debug("Response status: %d %s" % (response.status, response.read()))
                except Exception as e:
                    continue
                    logging.error(e)

                stoptime = time()
                requests = (requests + 1) % 3

                if score < 0.7:
                    continue

                if len(title) == len(mb_title):
                    title_distance = hamming(title, mb_title) / float(len(title))
                else:
                    title_distance = levenshtein(title, mb_title) / float(max(len(title), len(mb_title)))

                if len(artist) == len(mb_artists):
                    author_distance = hamming(artist, mb_artists) / float(len(artist))
                else:
                    author_distance = levenshtein(artist, mb_artists) / float(max(len(artist), len(mb_artists)))

                # if title_distance > 0.33 and author_distance > 0.5:
                logging.debug("distances: %s %s %s" % (score, title_distance, author_distance))
                #     continue

                logging.debug("puid: %s, mbid %s" % (puid, mbid))
                with self.condition:
                    try:
                        song_id, album_id = self.db.execute("select id, album_id from song where path = ?;", (path,)).fetchone()
                        self.db.execute("update song set puid = ?, mbid = ? where id = ?", (puid, mbid, song_id))
                        if title_distance > 0:
                            self.db.execute("update song set title = ? where id = ?", (mb_title, song_id))
                        self.db.commit()
                    except Exception as e:
                        logging.error(e)
        self.db.close()
