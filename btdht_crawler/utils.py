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
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpResponse
from django.contrib.auth import login

import os
import pymongo
import socket
import time
import json
import hashlib
from threading import Thread
from bson.binary import Binary
from datetime import datetime
from functools import wraps

from .scraper import scrape as scrape_one_tracker
import const

def token_auth(view):
    @wraps(view)
    def wrap(request, token, *args, **kwargs):
        try:
            user_pref = models.UserPref.objects.get(token=token)
            login(request, user_pref.user, backend=settings.BTDHT_TOKEN_AUTH_BACKEND)
            return view(request, *args, **kwargs)
        except models.UserPref.DoesNotExist:
            raise PermissionDenied()
    return wrap

def random_token():
    return hashlib.md5(os.urandom(16)).hexdigest()

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
    params["const"] = const

    return params


def getdb(collection="torrents_data"):
    try:
        return getdb.db[collection]
    except AttributeError:
        getdb.db = pymongo.MongoClient()[settings.BTDHT_MONGODB]
        return getdb.db[collection]



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


def format_date(timestamp, format='%Y-%m-%d %H:%M:%S'):
    return datetime.utcfromtimestamp(timestamp).strftime(format)


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
        bad_tracker = scrape.bad_tracker
        for tracker in bad_tracker.keys():
            if time.time() - bad_tracker[tracker] > (24 * 3600):
                del bad_tracker[tracker]
        threads = []
        results = {}
        for tracker in settings.BTDHT_TRACKERS:
            if tracker not in bad_tracker and tracker not in settings.BTDHT_TRACKERS_NO_SCRAPE:
                t = Thread(target=_scrape, args=(tracker, hashs_to_scrape, results))
                t.daemon = True
                t.start()
                threads.append(t)
        for t in threads:
            t.join()
        for tracker in settings.BTDHT_TRACKERS:
            if tracker not in results and tracker not in bad_tracker and tracker not in settings.BTDHT_TRACKERS_NO_SCRAPE:
                bad_tracker[tracker] = time.time()
        scrape.bad_tracker = bad_tracker
        scrape_result = {}
        for hash in hashs_to_scrape:
            scrape_result[hash] = {'complete': -1, 'peers': -1, 'seeds': -1}
        for tracker in results.values():
            for hash in tracker:
                 scrape_result[hash]['complete'] = max(scrape_result[hash]['complete'], tracker[hash]['complete'])
                 scrape_result[hash]['peers'] = max(scrape_result[hash]['peers'], tracker[hash]['peers'])
                 scrape_result[hash]['seeds'] = max(scrape_result[hash]['seeds'], tracker[hash]['seeds'])
        # delete hash where no results where returned
        for hash in hashs_to_scrape:
            if scrape_result[hash]['seeds'] < 0 or scrape_result[hash]['peers'] < 0 or scrape_result[hash]['complete'] < 0:
                del scrape_result[hash]
        result.update(scrape_result)
        now = int(time.time())
        for hash, value in scrape_result.items():
            value['last_scrape'] = now
            try:
                db.update({"_id": Binary(hash)}, {"$set": value})
            except pymongo.errors.PyMongoError:
                pass
        return result
scrape.bad_tracker = {}
    


def require_login(funct):
    if settings.BTDHT_REQUIRE_AUTH:
        return login_required(funct)
    else:
        return funct


def render_json(data):
    return HttpResponse(json.dumps(data, indent=True), content_type="application/json")


def absolute_url(request, path):
    return "%s://%s%s" % (request.scheme, request.get_host(), path)

import models
