#!/usr/bin/env python2.7
#
# A music library managaer and smart aggregator
# featuring http-based playlist request and automatic generation
#
# requirements: pysqlite, pylast, mutagen, pyinotify
# optional requirements: sox, soundstretch

__version__ = '0.7.0'
__author__ = 'radiocicletta <radiocicletta@gmail.com>'

from parameters import *  # a file parameters.py containing last.fm variables USERNAME and APIKEY
from metadata.remote import LastFMMetadataThread, DiscogsMetadataThread, FiledataThread
from metadata.fsutil import breadth_scan
from metadata.fs import create_subtreelistener
import sqlite3 as dbapi
import sys
import os
# from collections import defaultdict
import re
from getopt import getopt
import threading
from Queue import Queue
import signal
# import traceback
from time import sleep
import logging
from random import Random
from SocketServer import ThreadingTCPServer    # , ForkingTCPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
from StringIO import StringIO

DBSCHEMA = ("""
PRAGMA foreign_keys = ON;
""",
"""create table if not exists db (
    version text default "0.6",
    tree text not null
);""",
"""create table if not exists artist (
    id integer primary key asc autoincrement,
    name text not null
); """,
"""create table if not exists song (
    id integer primary key asc autoincrement,
    title text not null,
    titleclean text not null,
    genre_id references genre(id) not null default 1,
    album_id references album(id) not null default 1,
    artist_id references artist(id) not null default 1,
    trackno integer,
    puid text,
    bpm real,
    path text unique not null,
    length real default 0.0
);""",
"""create index songgenre on song(genre_id);
""",
"""create index songalbum on song(album_id);
""",
"""create index songartist on song(artist_id);
""",
"""create table if not exists genre (
    id integer primary key asc autoincrement,
    desc text not null,
    descclean text not null,
    weight real not null default 1,
    bpm real,
    bpmavg real
);""",
"""create table if not exists genre_x_genre (
    id_genre integer not null,
    id_related_genre integer not null,
    similarity real not null default 0.0
);""",
"""create table if not exists genre_x_tag (
    id_genre integer not null,
    id_tag integer not null,
    similarity real not null default 0.0
);""",
"""create table if not exists tag (
    id integer primary key asc autoincrement,
    name text unique not null,
    nameclean text not null
);
""",
"""create table if not exists song_x_tag (
    song_id references song(id),
    tag_id references tag(id),
    weight real not null
);
""",
"""create table album_x_genre (
    album_id references album(id),
    genre_id references genre(id),
    weight real not null
);
""",
"""create index songtagsong on song_x_tag(song_id);
""",
"""create index songtagtag on song_x_tag(tag_id);
""",
"""create table if not exists album (
    id integer primary key asc autoincrement,
    title text not null,
    titleclean text not null,
    date integer
);
""",
"""insert into genre (desc, descclean) values ('unknown', 'unknown') ;""",
"""insert into artist (name) values ('unknown') ;""",
"""insert into album (title, titleclean) values ('unknown', 'unknown') ;"""
)

THREADS = []


class PollAnalyzer(threading.Thread):
    """ Analyzer for genres. Collect informations about (lexicographically) similar genres """

    daemon = True

    def __init__(self, condition, dbpath):
        threading.Thread.__init__(self, name="PollAnalyzer")
        self.condition = condition
        self.dbpath = dbpath
        self.running = True

    def stop(self):
        self.running = False

    def run(self):

        sleeptime = 600
        complete_genres = 0

        def calcsimilarity(known, table, id1, id2, known_genres):
            splitted_genre = re.split("[/,;]", known_genres[i][1])
            ret = False
            for j in xrange(0, len(known)):
                if i == j:
                    continue
                if known_genres[i][2] == known[j][2]:
                    similarity = 1.0
                else:
                    compared_genre = re.split("[/,;]", known[j][1])
                    distance = {}
                    sametags = 0
                    for a in splitted_genre:
                        if not a:
                            continue
                        for b in compared_genre:
                            if not b or b in distance:
                                continue
                            if len(a) == len(b):
                                h = hamming(a, b) / float(len(a))
                                if h:
                                    distance[b] = h
                                else:
                                    sametags = sametags + 1
                            else:
                                distance[b] = levenshtein(a, b) / float(max(len(a), len(b)))
                    if distance:
                        # geometric mean + weighted equal tags
                        similarity = 1.0 - (reduce(lambda x, y: x * y, distance.values())) ** (1.0 / len(distance)) + (sametags / (sametags + len(distance)))
                    else:
                        similarity = 0.0
                if similarity > 0.33:
                    if not db.execute("select * from %s  where %s  = ? and %s = ?" % (table, id1, id2), (known_genres[i][0], known[j][0])).fetchall():
                        db.execute("insert into %s (%s, %s, similarity) values ( ?, ?, ?)" % (table, id1, id2), (known_genres[i][0], known[j][0], similarity))
                        ret = True
            return ret

        while self.running:
            sleep(max(300, sleeptime))
            logging.info("Performing tag/genres matching analysis")
            db = dbapi.connect(dbpath)
            self.condition.acquire()
            collected_genres = db.execute("select count(id) from genre;").fetchall()
            self.condition.release()

            genres_count = complete_genres - collected_genres[0][0]

            if genres_count == 0:
                sleeptime = (sleeptime + 6) % 36
                logging.debug("New Sleep time: %s Secs" % sleeptime)
            else:
                sleeptime = sleeptime / 2
                logging.debug("New Sleep time: %s Secs" % sleeptime)
                self.condition.acquire()
                known_genres = db.execute("select distinct id, desc, descclean from genre").fetchall()
                known_tags = db.execute("select distinct id, name, nameclean from tag").fetchall()
                self.condition.release()
                #commit = False
                for i in xrange(0, len(known_genres)):
                    if not self.running:
                        break
                    self.condition.acquire()

                    if calcsimilarity(known_genres, "genre_x_genre", "id_genre", "id_related_genre", known_genres) or calcsimilarity(known_tags, "genre_x_tag", "id_genre", "id_tag", known_genres):
                        db.commit()

                    complete_genres = db.execute("select count(id) from genre;").fetchall()[0][0]
                    self.condition.release()
                sleep(sleeptime)
            db.close()


class CatalogHTTPRequestHandler(SimpleHTTPRequestHandler):
    """
            a HTTPRequestHandler that perform queries on db
            the main idea is a conversion from URI to SQL:

            GET /<action>/[<subclause>[/<parameter>] ... ] => "SELECT FROM [joined tables] WHERE [clause from parameters]"

            query actions:
                /browse/ -- browse catalog like a listdir (e.g. /browse/genre/indie /browse/year/1979 /browse/genre/indie/year/1980)
                /search/ -- do a free text research (e.g. /search/blitzrieg%20bop)
                /smart/ -- perform a smart playlist (e.g. /smart/shine%20on%20you)
                /aggregate/ -- create a playlist based on a criteria (e.g. /aggregate/genre/indie,electro,indie-pop)
    """

    def do_GET(self):

        items = unquote(self.path).split("/")
        if len(items) < 2 or items == ['', '']:
            self.send_response(500)
            self.end_headers()
            return

        items = items[1:]
        if not items[-1]:
            items = items[:-1]

        db = dbapi.connect(self.server.dbpath)

        songquery = "select distinct s.id as id, s.title as title, a.name as artist, g.desc as genre, al.title as album, s.path as path, s.length as length, s.bpm as bpm from song s left join genre g on (s.genre_id = g.id) left join artist a on (s.artist_id = a.id) left join album al on (s.album_id = al.id)"
        results = ""

        try:
            if items[0] == "browse":
                where = []
                args = ()
                if len(items) % 2 == 0:
                    query = "select * from %s;" % items[-1]
                    results = self.text(db.execute(query).fetchall())
                    self.send_response(200)
                else:
                    for i in range(1, len(items), 2):
                        where.append("%s = ?" % items[i])
                        args = args + (items[i + 1],)
                    query = "%s %s order by s.title;" % (songquery, (where and "where %s" % " and ".join(where) or ""))
                    results = self.m3u(db.execute(query, args).fetchall())
                    self.send_response(200)

            elif items[0] == "search":
                    query = "%s where g.desc like ? or a.name like ? or s.title like ? or al.title like ? order by s.title;" % songquery
                    args = tuple('%%%s%%' % " ".join(items[1:]) for i in (1, 2, 3, 4))  # --> ('%%%s%%' % ... , '%%%s%%' % ... , '%%%s%%' % ... )

            elif items[0] == "smart":
                pass

            elif items[0] == "aggregate" and len(items) == 3:
                requests = items[2].split(',')
                if items[1] == 'genre':
                    ids = [i[0] for i in db.execute("select id from genre where desc in ( %s )" % ",".join(['?' for i in xrange(0, len(requests))]), requests).fetchall()]
                    query = "%s where s.genre_id in ( %s )" % (songquery, ",".join(['?' for i in xrange(0, len(ids))]))
                    primary_results = db.execute(query, ids).fetchall()
                    logging.debug("Primary results.")

                    related_requests = "select distinct id_related_genre from genre_x_genre where id_genre in ( %s ) and similarity > 0.98;" % ",".join(['?' for i in xrange(0, len(ids))])
                    related_ids = [i[0] for i in db.execute(related_requests, ids).fetchall()]

                    if related_ids:
                        related_query = "%s where s.genre_id in ( %s )" % (songquery, ",".join(['?' for i in xrange(0, len(related_ids))]))
                        secondary_results = db.execute(related_query, related_ids).fetchall()
                    else:
                        secondary_results = []
                    logging.debug("Secondary results.")

                    related_requests = "select distinct id_tag from genre_x_tag where id_genre in ( %s ) and similarity > 0.80;" % ",".join(['?' for i in xrange(0, len(ids))])
                    related_tags = [i[0] for i in db.execute(related_requests, ids).fetchall()]
                    logging.debug("Tags:")

                    tertiary_results = []
                    try:
                        if related_tags:
                            tags_requests = "select distinct song_id from song_x_tag where tag_id in ( %s ) and weight >= 50;" % ",".join(['?' for i in xrange(0, len(related_tags))])
                            tags_results = [i[0] for i in db.execute(tags_requests, related_tags).fetchall()]
                            if tags_results:
                                related_query = "%s where s.id in ( %s );" % (songquery, ",".join(['?' for i in xrange(0, len(tags_results))]))
                                tertiary_results = db.execute(related_query, tags_results).fetchall()
                    except Exception as e:
                        logging.debug(e)

                    merged_results = list(set(primary_results + secondary_results + tertiary_results))
                    Random().shuffle(merged_results)

                    results = self.m3u(merged_results)

                    self.send_response(200)
                elif items[1] == 'bpm':
                    query = "%s where bpm  between ? - 10 and ? + 10 ;"
                    results = self.m3u(db.execute(query, requests).fetchall())
                    self.send_response(200)
                elif items[1] == 'date':
                    pass
                else:
                    self.send_response(404)

            elif items[0] == "smart":
                pass

        except Exception as e:
            self.send_response(500)
            self.end_headers()
            results = str(e)

        data = StringIO()
        data.write(results)
        data.seek(0)

        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.copyfile(data, self.wfile)

        db.close()

    def do_HEAD(self):
        pass

    def m3u(self, songtuple):

        extm3u = """#EXTM3U\n%s\n""" % "\n".join(["""#EXTINF:%d,%s - %s\n%s""" % (i[6], i[2], i[1], i[5]) for i in songtuple])
        return unicode(extm3u).encode('utf-8')

    def text(self, songtuple):

        textdoc = "\n".join(["| %s |" % " | ".join([unicode(j).encode("utf-8") for j in i]) for i in songtuple])
        return textdoc


#class CatalogThreadingTCPServer(ForkingTCPServer):
class CatalogThreadingTCPServer(ThreadingTCPServer):
    """a threaded tcp server interfaced with a database"""

    allow_reuse_address = True

    def stop(self):
        self.shutdown()

    def __init__(self, server_address, RequestHandlerClass, dbpath, bind_and_activate=True):
        ThreadingTCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=True)
        #ForkingTCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=True)
        self.dbpath = dbpath


def daemonize(stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)    # Exit first parent.
    except OSError, e:
        sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    os.chdir("/")
    os.umask(0)
    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)    # Exit second parent.
    except OSError, e:
        sys.stderr.write("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    # Redirect standard file descriptors.
    si = open(stdin, 'r')
    so = open(stdout, 'a+')
    se = open(stderr, 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())


def start_daemon(path, dbpath, lf_queue, di_queue, fd_queue, condition):
    """ installs a subtree listener and wait for events """
    os.nice(19)

    notifier_sb = create_subtreelistener(path, dbpath, lf_queue, di_queue, fd_queue, condition)
    THREADS.append(notifier_sb)

    THREADS.append(PollAnalyzer(condition, dbpath))
    THREADS[-1].start()
    THREADS.append(CatalogThreadingTCPServer(("localhost", 8080), CatalogHTTPRequestHandler, dbpath))
    THREADS[-1].serve_forever()

    THREADS[-1].join()


def start_scan(path, db, lf_queue, di_queue, fd_queue, condition, depth=1):
    breadth_scan(path, db, lf_queue, di_queue, fd_queue, condition, depth)


def levenshtein(a, b):  # Dr. levenshtein, i presume.
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a, b = b, a
        n, m = m, n

    current = range(n + 1)
    for i in range(1, m + 1):
        previous, current = current, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete = previous[j] + 1, current[j - 1] + 1
            change = previous[j - 1]
            if a[j - 1] != b[i - 1]:
                change = change + 1
            current[j] = min(add, delete, change)

    return current[n]


def hamming(a, b):
    """ calculate the Hamming distance between a and b """
    return sum(c_a != c_b for c_a, c_b in zip(a, b))


def print_usage(argv):
    print("""
usage: %s [-h|--help] [-s|--scan] [-d|--daemonize] [-n|--no-recursive] [-v|--verbosity n]path\n
\t-h --help              print this help
\t-s --scan              scan path and prepare db
\t-l --listen            listen mode on path
\t-d --daemonize        daemonize "listen mode" on path
\t-n --no-recursive    do not scan subfolders
\t-b --no-bpm            do not perform bpm detection (faster)
\t-v --verbosity        set logging verbosity level
""" % argv)
    sys.exit(1)


def perror(reason):
    print("error %s\ntry -h option for usage" % reason)


def shutdown(signum, stack):
    for t in THREADS:
        try:
            t.stop()
        except:
            pass
    sys.exit(0)


if __name__ == "__main__":

    opts, args = getopt(sys.argv[1:], "hsldnbv:", ["help", "scan", "listen", "daemonize", "no-recursive", "no-bpm", "verbosity"])

    if not opts or not args:
        print_usage(sys.argv[0])
    elif len(args) > 1:
        perror("too much directories")
        sys.exit(1)
    else:
        recursive = True
        scan = False
        daemon = False
        listen = False
        bpmdetect = True
        verbosity = logging.WARNING
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                print_usage(sys.argv[0])
            elif opt in ("-s", "--scan"):
                scan = True
            elif opt in ("-l", "--listen"):
                listen = True
            elif opt in ("-d", "--daemonize"):
                daemon = True
            elif opt in ("-n", "--no-recursive"):
                recursive = False
            elif opt in ("-b", "--no-bpm"):
                bpmdetect = False
            elif opt in ("-v", "--verbosity"):
                verbosity = [logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][int(arg)]
            else:
                perror("reading from command line: %s" % opt[0])
                sys.exit(1)
        for arg in args:
            if not os.path.isdir(arg):
                perror("%s is not a valid directory." % arg)
                sys.exit(1)
            elif arg[-1:] == "/":
                patharg = arg[:-1]
                break
            else:
                patharg = arg
                break

        logging.basicConfig(filename="/tmp/cata.log", level=verbosity, format="%(asctime)s %(threadName)s (%(thread)d) %(levelname)s %(message)s")

        dbpath = "%s/.%s.sqlite" % (patharg, os.path.basename(patharg))
        prepare = not os.path.exists(dbpath)
        db = dbapi.connect(dbpath)
        if prepare:
            for sql in DBSCHEMA:
                db.execute(sql)
        lastfmqueue = Queue()  # deque() #SyncQueue()
        discogsqueue = Queue()  # deque() #SyncQueue()
        filedataqueue = Queue()  # deque() #SyncQueue()
        condition = threading.Condition()

        THREADS.append(LastFMMetadataThread(lastfmqueue, condition, dbpath))
        THREADS.append(DiscogsMetadataThread(discogsqueue, condition, dbpath))
        if bpmdetect:
            THREADS.append(FiledataThread(filedataqueue, condition, dbpath))

        if scan:
            THREADS[-1].start()
            THREADS[-2].start()
            THREADS[-3].start()
            start_scan(patharg, db, lastfmqueue, discogsqueue, filedataqueue, condition, recursive)
        if listen:
            signal.signal(signal.SIGTERM, shutdown)
            try:
                if daemon:
                    daemonize()
                THREADS[-1].start()
                THREADS[-2].start()
                THREADS[-3].start()
                start_daemon(patharg, dbpath, lastfmqueue, discogsqueue, filedataqueue, condition)
            except KeyboardInterrupt:
                os.kill(os.getpid(), signal.SIGTERM)
