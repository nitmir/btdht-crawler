#!/usr/bin/env python
import os
import sys
import time
import pymongo
import hashlib
import chardet
import functools
import json
import argparse
import progressbar
from bson.binary import Binary
from btdht import utils

import config

class TorrentNoName(ValueError):
    pass
class TorrentFileBadPathType(ValueError):
    pass

def widget(what=""):
    padding = 30
    return [progressbar.ETA(), ' ', progressbar.Bar('='), ' ', progressbar.SimpleProgress(), ' ' if what else "", what]

@functools.total_ordering
class TorrentFile(object):

    path = None
    size = None

    def __init__(self, file, encoding):
        path_key = b"path"
        file_path = []
        if b'path.utf-8' in file:
            path_key = b'path.utf-8'
            encoding = 'utf-8'
        for path_component in file[path_key]:
            if isinstance(path_component, int):
                file_path.append(str(path_component).decode())
            elif isinstance(path_component, bytes):
                try:
                    file_path.append(path_component.decode(encoding))
                except UnicodeDecodeError:
                    local_encoding = chardet.detect(path_component)['encoding']
                    if local_encoding is None:
                        local_encoding = "utf-8"
                    file_path.append(path_component.decode(local_encoding, 'ignore'))
            else:
                raise TorrentFileBadPathType("path element sould not be of type %s" % type(p).__name__)
        self.path = os.path.join(*file_path)
        self.size = file[b'length']

    def __str__(self):
        return self.path

    def __eq__(self, other):
        return self.path == other.path

    def __lt__(self, other):
        return self.path < other.path

    def serialize(self):
        return {'path': self.path, 'size': self.size}


class Torrent(object):

    hash = None
    name = None
    created = None
    files = None
    size = None
    files_nb = None

    def serialize(self):
        return {
            '_id': Binary(self.hash),
            'name': self.name,
            'created': self.created,
            'files': [file.serialize() for file in self.files] if self.files is not None else None,
            'size': self.size,
            'file_nb': self.files_nb
        }

    def __init__(self, path):
        self.path = path
        with open(path, 'r') as f:
            torrent = utils.bdecode(f.read())
        self.hash = hashlib.sha1(utils.bencode(torrent[b'info'])).digest()

        encoding = None
        if b'encoding' in torrent and torrent[b'encoding'].decode():
            encoding = torrent[b'encoding'].decode()
            if encoding in ['utf8 keys', 'mbcs']:
               encoding = "utf-8"
        else:
            if b'name' in torrent[b'info']:
                try:
                    encoding = chardet.detect(torrent[b'info'][b'name'])['encoding']
                except TypeError:
                    torrent[b'info'][b'name'] = str(torrent[b'info'][b'name'])
                    encoding = chardet.detect(torrent[b'info'][b'name'])['encoding']
            if not encoding:
                encoding = "utf-8"

        if b'name.utf-8' in torrent[b'info']:
            try:
                self.name = torrent[b'info'][b'name.utf-8'].decode("utf-8", 'ignore')
            except AttributeError:
                self.name = str(torrent[b'info'][b'name.utf-8']).decode("utf-8", 'ignore')
        elif b'name' in torrent[b'info']:
            self.name = torrent[b'info'][b'name'].decode(encoding, 'ignore')
        else:
            raise TorrentNoName(self.hash.encode("hex"))

        try:
            self.created = int(torrent.get(b'creation date', int(time.time())))
        except ValueError as e:
            self.created = int(time.time())

        if b'files' in torrent[b'info']:
            self.files_nb = len(torrent[b'info'][b'files'])
            self.size = sum([file[b'length'] for file in torrent[b'info'][b'files']])
            files = []
            # only store the 1000 first files on the torrent
            for file in torrent[b'info'][b'files'][:1000]:
                try:
                    files.append(TorrentFile(file, encoding))
                except (TorrentFileBadPathType, LookupError):
                    pass
            files.sort()
            self.files = files
        else:
            self.files_nb = 1
            self.size = torrent[b'info'][b'length']

    def done_move(self):
        hex_hash = self.hash.encode("hex")
        path_dir = os.path.join(config.torrents_done, hex_hash[0], hex_hash[1], hex_hash[2], hex_hash[3])
        path = os.path.join(path_dir, "%s.torrent" % hex_hash)
        dir = config.torrents_done
        if not os.path.isdir(path_dir):
            for i in range(5): # 0 1 2 3 4
                dir = os.path.join(dir, hex_hash[i])
                try:
                    os.mkdir(dir)
                except OSError as error:
                    if error.errno != 17: # File exists
                        raise
        try:
            os.rename(self.path, path)
        except OSError:
            print "path %s or %s errored" % (self.path, path)
            raise
        self.path = path
        
        


class Manager(object):

    last_process = 0

    def __init__(self, progress=False):
        self.db1 = pymongo.MongoClient()[config.mongo["db"]]["torrents"]
        self.db2 = pymongo.MongoClient()[config.mongo["db"]]["torrents_data"]
        self.db3 = pymongo.MongoClient()[config.mongo["db"]]["torrents_stats"]
        self.progress = progress

    def add_stats(self, force=False):
        last_stats = self.db3.find().sort([('_id', -1)]).limit(1).next()
        if force or time.time() - last_stats['_id'] >= 1800:
            hash_tracked = self.db1.find({"status":{"$in": [0, None]}}).count()
            torrent_indexed = self.db2.find().count()
            print "Record stats: trackings %s hash, indexed %s torrents" % (hash_tracked, torrent_indexed)
            self.db3.insert({"_id": int(time.time()), "hash_tracked": hash_tracked, "torrent_indexed": torrent_indexed})

    def clean_db(self):
        sys.stdout.write("Deleting old torrents from db... ")
        one_hour_ago = int(time.time()) - 3600
        result1 = self.db1.delete_many({"dht_last_get": {"$lt": one_hour_ago}, "status":{"$in": [0, None]}})
        result2 = self.db1.delete_many({"dht_last_announce": {"$lt": one_hour_ago}, "status":{"$in": [0, None]}})
        print "%s deleted" % (result1.deleted_count + result2.deleted_count)

    def process_new_torrents(self):
        i = 0
        files = os.listdir(config.torrents_dir)
        if self.progress:
            pbar = progressbar.ProgressBar(widgets=widget("added new torrents"), maxval=len(files)).start()
        else:
            sys.stdout.write("Adding new torrents... ")
            sys.stdout.flush()
        for file in files:
            if file.endswith(".torrent"):
                i += 1
                torrent_path = os.path.join(config.torrents_dir, file)
                self._process_torrent(torrent_path)
            if self.progress:
                pbar.update(pbar.currval + 1)
        if self.progress:
            pbar.finish()
        else:
            print "%s added" % i

    def _process_torrent(self, path):
        torrent = Torrent(path)
        self.db2.update({'_id': Binary(torrent.hash)}, torrent.serialize(), upsert=True)
        self.db1.update({'_id': Binary(torrent.hash)}, {"$set": {"status": 2}}, upsert=True)
        torrent.done_move()

    def reprocess_done_torrents(self):
        for file1 in os.listdir(config.torrents_done):
            path1 = os.path.join(config.torrents_done, file1)
            for file2 in os.listdir(path1):
                path2 = os.path.join(path1, file2)
                for file3 in os.listdir(path2):
                    path3 = os.path.join(path2, file3)
                    for file4 in os.listdir(path3):
                        path4 = os.path.join(path3, file4)
                        for file in os.listdir(path4):
                            path = os.path.join(path4, file)
                            if path.endswith(".torrent"):
                                print path
                                self._process_torrent(path)

    def process_args(self, args):
        self.last_process = int(time.time())
        if args.add_new_torrents or args.all:
            self.process_new_torrents()
        if args.clean_db or args.all:
            self.clean_db()
        if args.record_stats or args.all:
            self.add_stats(force=args.record_stats)
        if args.reprocess_done_torrents:
            self.reprocess_done_torrents()

    def sleep(self, sleep):
        sleep_time = int(max(0, sleep - (time.time() - self.last_process)))
        if sleep_time > 0:
            print("Now spleeping until the next loop")
            if self.progress:
                pbar = progressbar.ProgressBar(widgets=sleep_widget, maxval=sleep_time).start()
                for i in xrange(sleep_time):
                    time.sleep(1)
                    pbar.update(i+1)
                pbar.finish()
            else:
                time.sleep(sleep_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--progress", "-P", help="display a progress bar for each action", action="store_true")
    parser.add_argument("--add-new-torrents", "-A", help="process new torrents and add them to the database", action="store_true")
    parser.add_argument("--clean-db", "-D", help="Delete old hash from the database", action="store_true")
    parser.add_argument("--record-stats", "-S", help="Add a stats record", action="store_true")
    parser.add_argument("--reprocess-done-torrents", help="Try to readd to database already processed torrents", action="store_true")
    parser.add_argument("--all", help="add new torrents, Delete old hash, and Add a stats record if needed. Kind of -A -D -S equivalent", action="store_true")
    parser.add_argument("--loop", help="Loop actions every 5 minutes", action="store_true")
    args = parser.parse_args()
    manager = Manager(args.progress)
    if not args.loop:
        manager.process_args(args)
    else:
        sleep_widget = [progressbar.Bar('>'), ' ', progressbar.ETA(), ' ', progressbar.ReverseBar('<')]
        while True:
            try:
                manager.process_args(args)
            except pymongo.errors.PyMongoError as error:
                print "PyMongoError: %s" %error
            manager.sleep(60)
