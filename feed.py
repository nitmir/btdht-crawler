#!/usr/bin/env python
import os
import sys
import time
import pymongo
import hashlib
import chardet
import functools
import argparse
import progressbar
import mimetypes
import collections
from bson.binary import Binary
from btdht import utils

import config
import categories
from btdht_search.scraper import scrape_max


class TorrentNoName(ValueError):
    pass


class TorrentFileBadPathType(ValueError):
    pass


def widget(what=""):
    return [
        progressbar.ETA(), ' ', progressbar.Bar('='), ' ', progressbar.SimpleProgress(),
        ' ' if what else "", what
    ]


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
                raise TorrentFileBadPathType(
                    "path element sould not be of type %s" % type(path_component).__name__
                )
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
    added = None

    def serialize(self):
        return {
            '_id': Binary(self.hash),
            'name': self.name,
            'created': self.created,
            'files': [file.serialize() for file in self.files] if self.files is not None else None,
            'size': self.size,
            'file_nb': self.files_nb,
            'added': self.added
        }

    def __init__(self, path):
        self.path = path
        self.added = time.time()
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
        except ValueError:
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
        path_dir = os.path.join(
            config.torrents_done,
            hex_hash[0],
            hex_hash[1],
            hex_hash[2],
            hex_hash[3]
        )
        path = os.path.join(path_dir, "%s.torrent" % hex_hash)
        dir = config.torrents_done
        if not os.path.isdir(path_dir):
            for i in range(4):  # 0 1 2 3
                dir = os.path.join(dir, hex_hash[i])
                try:
                    os.mkdir(dir)
                except OSError as error:
                    if error.errno != 17:  # File exists
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
            torrent_indexed = self.db2.find().count()
            data = {"_id": int(time.time()), "torrent_indexed": torrent_indexed}
            for cat in categories.categories:
                data[cat] = self.db2.find({'categories': cat}).count()
            print "Record stats: indexed %s torrents" % torrent_indexed
            self.db3.insert(data)

    def process_new_torrents(self, scrape=False):
        i = 0
        files = os.listdir(config.torrents_dir)
        hashes = []
        results = {}
        if not files:
            return
        if self.progress:
            pbar = progressbar.ProgressBar(
                widgets=widget("added new torrents"),
                maxval=len(files)
            ).start()
        else:
            sys.stdout.write("Adding new torrents... ")
            sys.stdout.flush()
        for file in files:
            if file.endswith(".torrent"):
                i += 1
                torrent_path = os.path.join(config.torrents_dir, file)
                torrent = self._process_torrent(torrent_path)
                if scrape:
                    hashes.append(torrent.hash)
                    if len(hashes) > 73:
                        results.update(scrape_max(config.scrape_trackers, hashes)[1])
                        hashes = []
            if self.progress:
                pbar.update(pbar.currval + 1)
        if self.progress:
            pbar.finish()
        else:
            print "%s added" % i
        if scrape:
            sys.stdout.write("Scraping new torrents...")
            sys.stdout.flush()
            results.update(scrape_max(config.scrape_trackers, hashes)[1])
            now = int(time.time())
            for hash, value in results.items():
                value['last_scrape'] = now
                try:
                    self.db2.update({"_id": Binary(hash)}, {"$set": value})
                except pymongo.errors.PyMongoError:
                    pass
            print "OK"

    def _process_torrent(self, path):
        try:
            torrent = Torrent(path)
            self.db2.update({'_id': Binary(torrent.hash)}, torrent.serialize(), upsert=True)
            self.db1.update({'_id': Binary(torrent.hash)}, {"$set": {"status": 2}}, upsert=True)
            torrent.done_move()
        except utils.BcodeError:
            os.rename(path, os.path.join(config.torrents_error, os.path.basename(path)))
        return torrent

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
            self.process_new_torrents(scrape=args.scrape)
        if args.record_stats or args.all:
            self.add_stats(force=args.record_stats)
        if args.categorise or args.categorise_all or args.all:
            self.categorise(all=args.categorise_all)
        if args.reprocess_done_torrents:
            self.reprocess_done_torrents()
        if args.mimes_report:
            self.mimes_report()

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

    def _categorise(self, result, filter=True):
        cat = collections.defaultdict(int)
        if result.get('files') is not None:
            files = result['files']
        else:
            files = [{'path': result['name'], 'size': result['size']}]
        for file in files:
            if file['size'] > 0:
                typ = categories.guess(file['path'])
                if typ is None:
                    typ = 'other'
                if typ != 'other':
                    cat[typ] += file['size']
                else:
                    cat[typ] += 1
        cat_list = cat.items()
        cat_list.sort(key=lambda x: -x[1])
        if filter is False:
            return cat_list
        cat_list = [c[0] for c in cat_list] or ['other']
        max = cat[cat_list[0]]
        return [c for c in cat_list if cat[c] >= (max / 4.0)]

    def categorise(self, all=False):
        if all:
            results = self.db2.find({})
        else:
            results = self.db2.find({'categories': {'$in': [None, []]}})
        maxval = results.count()
        if maxval == 0:
            return
        if self.progress:
            pbar = progressbar.ProgressBar(
                widgets=widget("torrent categorised"),
                maxval=maxval
            ).start()
        for result in results:
            cats = self._categorise(result)
            self.db2.update({'_id': result['_id']}, {'$set': {'categories': cats}})
            if self.progress:
                try:
                    pbar.update(pbar.currval + 1)
                except:
                    pass

        if self.progress:
            pbar.finish()

    def mimes_report(self):
        results = self.db2.find({}, {'files': True, 'name': True})
        mimes = collections.defaultdict(int)
        not_known = collections.defaultdict(int)
        if self.progress:
            pbar = progressbar.ProgressBar(widgets=widget(), maxval=results.count()).start()
        for result in results:
            if not result['files']:
                result['files'] = [{'path': result['name']}]
            for file in result['files']:
                    mime = mimetypes.guess_type(file['path'], strict=False)[0]
                    typ = None
                    if mime:
                        typ = categories.mime_to_category(mime)
                        if typ is None:
                            mimes[mime] += 1
                    if typ is None:
                        ext = os.path.splitext(file['path'])[1].lower()
                        if ext and categories.extension_to_category(ext) is None:
                            not_known[ext] += 1
            if self.progress:
                try:
                    pbar.update(pbar.currval + 1)
                except:
                    pass
        if self.progress:
            pbar.finish()
        mimes = mimes.items()
        mimes.sort(key=lambda x: -x[1])
        not_known = not_known.items()
        not_known.sort(key=lambda x: -x[1])
        with open('mime_types.txt', 'w') as mime_f, open('extensions.txt', 'w') as ext_f:
            for (value, nb) in mimes:
                mime_f.write(value.encode("utf-8"))
                mime_f.write(': ')
                mime_f.write(str(nb))
                mime_f.write('\n')
            for (value, nb) in not_known:
                ext_f.write(value.encode("utf-8"))
                ext_f.write(': ')
                ext_f.write(str(nb))
                ext_f.write('\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--progress", "-P",
        help="display a progress bar for each action",
        action="store_true"
    )
    parser.add_argument(
        "--add-new-torrents", "-A",
        help="process new torrents and add them to the database",
        action="store_true"
    )
    parser.add_argument(
        "--record-stats", "-S",
        help="Add a stats record",
        action="store_true"
    )
    parser.add_argument(
        "--reprocess-done-torrents",
        help="Try to readd to database already processed torrents",
        action="store_true"
    )
    parser.add_argument(
        "--all",
        help=(
            "add new torrents, Delete old hash, and Add a stats record if needed. "
            "Kind of -A -D -S equivalent"
        ),
        action="store_true")
    parser.add_argument(
        "--loop",
        help="Loop actions every minutes",
        type=int
    )
    parser.add_argument(
        "--mimes-report",
        help=(
            "Generate report on not classified files extensions and mime "
            "types (extensions.txt and mime_types.txt)"
        ),
        action="store_true"
    )
    parser.add_argument(
        "--categorise",
        help="Compute torrents categories for new torrents",
        action="store_true"
    )
    parser.add_argument(
        "--categorise-all",
        help="Compute torrents categories for all torrents",
        action="store_true"
    )
    parser.add_argument(
        "--scrape",
        help="Scrape new torrents when called with --add-new-torrents",
        action="store_true"
    )
    args = parser.parse_args()
    manager = Manager(args.progress)
    if args.loop is None:
        manager.process_args(args)
    else:
        sleep_widget = [
            progressbar.Bar('>'), ' ', progressbar.ETA(), ' ', progressbar.ReverseBar('<')
        ]
        while True:
            try:
                manager.process_args(args)
            except pymongo.errors.PyMongoError as error:
                print "PyMongoError: %s" % error
            manager.sleep(args.loop)
