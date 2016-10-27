#!/usr/bin/env python

import os
import sys
import pymongo
import urllib
import time
import progressbar
import subprocess
import argparse
from btdht.utils import bdecode
from threading import Thread
from bson.binary import Binary

import config

def decode_leechers_paradise_hash(h):
    r=[]
    i=0
    l = len(h)
    while i < l:
        if h[i] == '%':
            r.append(h[i+1:i+3].decode("hex"))
            i+=3
        else:
            r.append(h[i])
            i+=1
    return "".join(r)

def widget(what=""):
    padding = 30
    return [progressbar.ETA(), ' ', progressbar.Bar('='), ' ', progressbar.SimpleProgress(), ' ' if what else "", what]

tracker_scrape_url = {
    'leechers-paradise': ("http://scrape.leechers-paradise.org/static_scrape", 900),
    'zer0day': ("http://zer0day.to/fullscrape.gz", 1800),
    'coppersurfer': ("http://coppersurfer.tk/full_scrape_not_a_tracker.tar.gz", 3600),
    'sktorrent': ("http://tracker.sktorrent.net/full_scrape_not_a_tracker.tar.gz", 1800),
    'pirateparty': ("http://tracker.pirateparty.gr/full_scrape.tar.gz", 3600),
}

class Scraper(object):

    def __init__(self):
        self.db = pymongo.MongoClient()[config.mongo["db"]]["torrents_data"]

    def load(self, name, hashes):
        base_dir = os.path.join(config.torrents_scrape, name)
        if tracker_scrape_url[name][0].endswith(".tar.gz"):
            path = os.path.join(base_dir, "scrape")
        elif tracker_scrape_url[name][0].endswith(".gz"):
            path = os.path.join(base_dir, os.path.basename(tracker_scrape_url[name][0])[:-3])
        else:
            path = os.path.join(base_dir, os.path.basename(tracker_scrape_url[name][0]))
        if name == 'leechers-paradise':
            files = {}
            with open(path) as f:
                for line in f:
                    hash, seeds, peers = line.strip().split(':')
                    try:
                        hash = decode_leechers_paradise_hash(hash)
                        if hash in hashes:
                            files[hash] = (int(peers), int(seeds), -1)
                    except TypeError as error:
                        sys.stderr.write("%r: %s\n" % (error, hash))
            return files
        else:
            files = {}
            with open(path) as f:
                data = bdecode(f.read())['files']
            for hash, value in data.iteritems():
                if hash in hashes:
                    files[hash] = (value['incomplete'], value['complete'], value['downloaded'])
            return files

    def _download(self, name, url, path):
        sys.stdout.write(" %s" % name)
        sys.stdout.flush()
        dl = urllib.URLopener()
        dl.retrieve(url, path)
        if path.endswith(".tar.gz"):
            p = subprocess.Popen(['tar', 'xf', os.path.basename(path)], cwd=os.path.dirname(path))
            p.wait()
            os.remove(path)
        elif path.endswith(".gz"):
           p = subprocess.Popen(['gunzip', '-f', os.path.basename(path)], cwd=os.path.dirname(path))
           p.wait()
        sys.stdout.write(".")
        sys.stdout.flush()

    def download(self):
        threads = []
        sys.stdout.write("Downloading scrape infos from")
        sys.stdout.flush()
        for (name, (url, refresh)) in tracker_scrape_url.items():
            base_dir = os.path.join(config.torrents_scrape, name)
            if not os.path.isdir(base_dir):
                os.mkdir(base_dir)
            file_path = os.path.join(base_dir, os.path.basename(url))
            if file_path.endswith(".tar.gz"):
                stat_file = os.path.join(base_dir, "scrape")
            elif file_path.endswith(".gz"):
                stat_file = file_path[:-3]
            else:
                stat_file = file_path
            if not os.path.isfile(stat_file) or time.time() - os.stat(stat_file).st_mtime > refresh:
                t = Thread(target=self._download, args=(name, url, file_path))
                t.daemon = True
                t.start()
                threads.append(t)
        sys.stdout.write(": ")
        sys.stdout.flush()
        for t in threads:
            t.join()
        print "OK"

    def full_scrape(self):
        sys.stdout.write("Fetching hashs from database...")
        sys.stdout.flush()
        hashs = set(str(h['_id']) for h in self.db.find({}, {'_id': True}))
        no_scrape_data_hashes = set(str(h['_id']) for h in self.db.find(
            {"$or": [
                {'last_scrape': {'$in': [0, None]}},
                {'peers': {'$in': [-1, None]}},
                {'seeds': {'$in': [-1, None]}},
            ]}
            , {'_id': True}
        ))
        print "OK"
        now = int(time.time())
        print now
        trackers = []
        print "Loading scrape data from:"
        for name in tracker_scrape_url:
            sys.stdout.write(" * %s..." % name)
            sys.stdout.flush()
            trackers.append(self.load(name, hashs))
            print "OK"
        pbar = progressbar.ProgressBar(widgets=widget("scraping torrents"), maxval=len(hashs)).start()
        for hash in hashs:
            peers = -1
            seeds = -1
            complete = -1
            for tracker in trackers:
                try:
                    values = tracker[hash]
                    peers = max(peers, values[0])
                    seeds = max(seeds, values[1])
                    complete = max(complete, values[2])
                except KeyError:
                    pass
            if peers > -1 and seeds > -1 or hash in no_scrape_data_hashes:
                data = {"peers": peers, "seeds": seeds, "last_scrape": now}
                if complete > -1:
                    data["complete"] = complete
                self.db.update({'_id': Binary(hash)}, {"$set": data})
                pbar.update(pbar.currval + 1)
        pbar.finish()

    def incremental_scrape(self):
        hashes = set(str(h['_id']) for h in self.db.find({'last_scrape': {'$in': [0, None]}}, {'_id': True}))
        print len(hashes)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full-scrape", help="Scrape all torrent in the database from downloaded scrape files", action="store_true")
    parser.add_argument("--download-scrape", help="Download scrape files from configured torrent sites", action="store_true")
    parser.add_argument("--incremental-scrape", help="Scrape new torrent not previously scraped", action="store_true")
    args = parser.parse_args()
    scraper = Scraper()
    if args.download_scrape:
        scraper.download()
    if args.full_scrape:
        scraper.full_scrape()
    if args.incremental_scrape:
        scraper.incremental_scrape()
