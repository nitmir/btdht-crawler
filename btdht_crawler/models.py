from __future__ import unicode_literals
from .settings import settings

from django.db import models
from django.urls import reverse

import os
import urllib
import time
import re
from datetime import datetime, timedelta
from bson.binary import Binary

from .utils import getdb, format_size, scrape

class TorrentsList(object):

    def __init__(self, cursor, url, page=1):
        skip = settings.BTDHT_PAGE_SIZE * (page - 1)
        limit = settings.BTDHT_PAGE_SIZE
        self.page = page
        self._cursor = cursor.skip(skip).limit(limit)
        self.size = cursor.count()
        self.start = skip
        self.end = skip + limit if limit > 0 else self.size

        self.last_page = int(self.size/settings.BTDHT_PAGE_SIZE) + 1

        self.start_page = max(1, page - 26)
        self.end_page = min(self.last_page, page + 26)

        self.url = url

    def __iter__(self):
        torrents = []
        to_scrape = []
        for result in self._cursor:
            torrents.append(result)
            if not 'last_scrape' in result or result['last_scrape'] == 0 or (time.time() - result['last_scrape']) > settings.BTDHT_SCRAPE_MIN_INTERVAL:
                to_scrape.append(str(result['_id']))
        if to_scrape:
            scrape_result = scrape(to_scrape)
            for result in torrents:
                result.update(scrape_result.get(str(result['_id']), {}))
                yield Torrent(obj=result, no_files=True)
        else:
            for result in torrents:
                yield Torrent(obj=result, no_files=True)

    def pages(self):
        if self.has_previous_page():
            yield {'url': self.url(self.page - 1), 'class': "pagination-prev", 'name': "&laquo;"}
        else:
            yield {'url': None, 'class': "pagination-prev disabled", 'name': "&laquo;"}
        if self.show_start_suspension():
            yield {
                'url': self.url(1),
                'class': "active" if self.page == 1 else None,
                'name': 1
            }
            yield {'url': None, 'class': "disabled", 'name': "..."}
        for i in xrange(self.start_page, self.end_page + 1):
            yield {
                'url': self.url(i),
                'class': "active" if self.page == i else None,
                'name': i
            }
        if self.show_end_suspension():
            yield {'url': None, 'class': "disabled", 'name': "..."}
            yield {
                'url': self.url(self.last_page), 
                'class': "active" if self.page == self.last_page else None,
                'name': self.last_page
            }
        if self.has_next_page():
            yield {'url': self.url(self.page + 1), 'class': "pagination-next", 'name': "&raquo;"}
        else:
            yield {'url': None, 'class': "pagination-next disabled", 'name': "&raquo;"}

    def has_previous_page(self):
        return self.page > 1

    def has_next_page(self):
        return self.page < self.last_page

    def show_start_suspension(self):
        return self.start_page > 2

    def show_end_suspension(self):
        return self.end_page < (self.last_page - 1)
        


# Create your models here.
class Torrent(object):

    score = None

    hash = None
    name = None
    files = None
    created = None
    file_nb = None
    size = None

    seeds = None
    peers = None
    complete = None

    last_scrape = 0

    @staticmethod
    def search(query, page=1):
        db = getdb()
        if re.match("^[0-9A-Fa-f]{40}$", query):
            results = db.find(
                {"$or": [
                    {"$text": {"$search": query}},
                    {"_id": query}
                ]},
                {"score": {"$meta": "textScore" }, 'files': False}
               
            ).sort([("score", {"$meta": "textScore" })])
        else:
            results = db.find(
                {"$text": {"$search": query}},
                {"score": {"$meta": "textScore" }}
            ).sort([("score", {"$meta": "textScore" })])
        return TorrentsList(results, url=lambda page:reverse("btdht_crawler:index_query", args=[page, query]), page=page)

    @staticmethod
    def recent(page):
        db = getdb()
        results = db.find(
            {},
            {'files': False}
        ).sort(
            [("created", -1)]
        )
        return TorrentsList(results, url=lambda page:reverse("btdht_crawler:recent", args=[page]), page=page)

    def __init__(self, hash=None, obj=None, no_files=False):
        if obj is None and hash is not None:
            db = getdb()
            results = db.find({"_id": Binary(hash)})
            if results.count() != 1:
                raise ValueError("Torrent for hash %r not found" % hash)
            obj = results[0]
        if obj is not None:
            self.score = obj.get("score")
            self.hash = obj['_id']
            self.name = obj['name']
            self.size = obj['size']
            self.created = obj['created']
            self.file_nb = obj['file_nb']
            if no_files is False:
                self.files = obj['files']

            self.seeds = obj.get('seeds')
            self.peers = obj.get('peers')
            self.complete = obj.get('complete')
            self.last_scrape = obj.get('last_scrape', 0)
        else:
            raise ValueError("missing value to initialize Torrent object")

    def scrape(self):
        if time.time() - self.last_scrape > settings.BTDHT_SCRAPE_MIN_INTERVAL:
            result = scrape([self.hash])
            self.seeds = result[self.hash]['seeds']
            self.peers = result[self.hash]['peers']
            self.complete = result[self.hash]['complete']
            self.last_scrape = result[self.hash]['last_scrape']

    @property
    def hex_hash(self):
        return self.hash.encode("hex").lower()

    @property
    def magnet(self):
        trackers = "&".join("tr=%s" % urllib.quote(t) for t in settings.BTDHT_TRACKERS)
        return "magnet:?xt=urn:btih:%s&db=%s&%s" % (self.hex_hash, self.name, trackers)

    @property
    def path(self):
        hex_hash = self.hex_hash
        return os.path.join(
            settings.BTDHT_TORRENTS_BASE_PATH,
            hex_hash[0],
            hex_hash[1],
            hex_hash[2],
            hex_hash[3],
            "%s.torrent" % hex_hash
        )

    @property
    def url(self):
        if os.path.isfile(self.path):
            return reverse("btdht_crawler:download_torrent", args=[self.hex_hash, self.name])
        else:
            return None

    @property
    def size_pp(self):
        return format_size(self.size)

    @property
    def created_pp(self):
        return datetime.fromtimestamp(self.created).strftime('%Y-%m-%d %H:%M:%S')

    @property
    def created_delta(self):
        return timedelta(seconds=int(time.time()) - self.created)
