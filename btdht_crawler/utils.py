# -*- coding: utf-8 -*-
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License version 3 for
# more details.
#
# You should have received a copy of the GNU General Public License version 3
# along with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# (c) 2015-2016 Valentin Samir
"""Some util function for the app"""
from .settings import settings

from django.contrib.messages import constants as DEFAULT_MESSAGE_LEVELS

import pymongo
import socket
import time
from threading import Thread
from bson.binary import Binary

from .scraper import scrape as scrape_one_tracker

def context(params):
    """
        Function that add somes variable to the context before template rendering

        :param dict params: The context dictionary used to render templates.
        :return: The ``params`` dictionary with the key ``settings`` set to
            :obj:`django.conf.settings`.
        :rtype: dict
    """
    params["settings"] = settings
    params["message_levels"] = DEFAULT_MESSAGE_LEVELS

    return params


def getdb():
    try:
        return getdb.db
    except AttributeError:
        getdb.db = pymongo.MongoClient()[settings.BTDHT_MONGODB]["torrents_data"]
        return getdb.db


def format_size(i):
    if i > 1024**4:
        return "%s TB" % round(i/(1024.0**4), 2)
    elif i > 1024**3:
        return "%s GB" % round(i/(1024.0**3), 2)
    elif i > 1024**2:
        return "%s MB" % round(i/(1024.0**2), 2)
    elif i > 1024**1:
        return "%s KB" % round(i/(1024.0**1), 2)
    else:
        return "%s B" % i


def _scrape(tracker, hashs, results):
    try:
        results[tracker] = scrape_one_tracker(tracker, hashs)
    except (socket.timeout, socket.gaierror):
        pass

def scrape(hashs, refresh=False):
    db = getdb()
    result = {}
    if refresh is False:
         db_results = db.find(
             {'$or': [{"_id": Binary(hash)} for hash in hashs]},
             {'seeds': True, 'peers': True, 'complete': True, 'last_scrape': True}
         )
         db_hashs = {}
         for r in db_results:
             if 'last_scrape' in r:
                 if time.time() - r['last_scrape'] <= settings.BTDHT_SCRAPE_MIN_INTERVAL:
                     result[str(r['_id'])] = {'seeds': r['seeds'], 'peers': r['peers'], 'complete': r['complete'], 'last_scrape': r['last_scrape']}
    hashs_to_scrape = [h for h in hashs if h not in result]
    if hashs_to_scrape:
        try:
            bad_tracker = scrape.bad_tracker
        except AttributeError:
            bad_tracker = {}
        for tracker in bad_tracker.keys():
            if time.time() - bad_tracker[tracker] > 600:
                del bad_tracker[tracker]
        threads = []
        results = {}
        for tracker in settings.BTDHT_TRACKERS:
            if tracker not in bad_tracker:
                t = Thread(target=_scrape, args=(tracker, hashs_to_scrape, results))
                t.daemon = True
                t.start()
                threads.append(t)
        for t in threads:
            t.join()
        for tracker in settings.BTDHT_TRACKERS:
            if tracker not in results and tracker not in bad_tracker:
                bad_tracker[tracker] = time.time()
        scrape.bad_tracker = bad_tracker
        scrape_result = {}
        for hash in hashs_to_scrape:
            scrape_result[hash] = {'complete': 0, 'peers': 0, 'seeds': 0}
        for tracker in results.values():
            for hash in tracker:
                 scrape_result[hash]['complete'] = max(scrape_result[hash]['complete'], tracker[hash]['complete'])
                 scrape_result[hash]['peers'] = max(scrape_result[hash]['peers'], tracker[hash]['peers'])
                 scrape_result[hash]['seeds'] = max(scrape_result[hash]['seeds'], tracker[hash]['seeds'])
        result.update(scrape_result)
        now = int(time.time())
        for hash, value in scrape_result.items():
            value['last_scrape'] = now
            db.update({"_id": Binary(hash)}, {"$set": value})
    return result
    
