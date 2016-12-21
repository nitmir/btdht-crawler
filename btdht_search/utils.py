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
from django.contrib.gis.geoip2 import GeoIP2

import os
import pymongo
import time
import json
import hashlib
import re
import netaddr
from six.moves import urllib
from bson.binary import Binary
from datetime import datetime
from functools import wraps
from geoip2.errors import AddressNotFoundError
import pytz

from .scraper import scrape_max
import const

geoip2 = GeoIP2()

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

def context(request, params):
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
    try:
        params["country_code"] = geoip2.country_code(request.META['REMOTE_ADDR'])
    except (KeyError, AddressNotFoundError):
        params["country_code"] = None
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


def format_date(timestamp, format='%Y-%m-%d %H:%M:%S', timezone='UTC'):
    try:
        tzlocal = pytz.timezone(urllib.parse.unquote(timezone))
    except pytz.UnknownTimeZoneError:

        tzlocal = pytz.utc
    tzutc = pytz.utc
    return datetime.utcfromtimestamp(
        timestamp
    ).replace(tzinfo=tzutc).astimezone(tzlocal).strftime(format)


def scrape(hashs, refresh=False):
    db = getdb()
    result = {}
    if refresh is False:
        db_results = db.find(
            {'$or': [{"_id": Binary(hash)} for hash in hashs]},
            {'seeds': True, 'peers': True, 'complete': True, 'last_scrape': True}
        )
        for r in db_results:
            if 'last_scrape' in r:
                if time.time() - r['last_scrape'] <= settings.BTDHT_SCRAPE_MIN_INTERVAL:
                    result[str(r['_id'])] = {
                        'seeds': r['seeds'],
                        'peers': r['peers'],
                        'complete': r['complete'],
                        'last_scrape': r['last_scrape']
                    }
    hashs_to_scrape = [h for h in hashs if h not in result]
    if hashs_to_scrape:
        bad_tracker = scrape.bad_tracker
        for tracker in bad_tracker.keys():
            if time.time() - bad_tracker[tracker] > 600:
                del bad_tracker[tracker]
        good_trackers = [
            tracker for tracker in settings.BTDHT_TRACKERS
            if tracker not in bad_tracker and tracker not in settings.BTDHT_TRACKERS_NO_SCRAPE
        ]
        (result_trackers, scrape_result) = scrape_max(
            good_trackers,
            hashs_to_scrape,
            udp_timeout=2.5
        )
        for tracker in settings.BTDHT_TRACKERS:
            if (
                tracker not in result_trackers and
                tracker not in bad_tracker and
                tracker not in settings.BTDHT_TRACKERS_NO_SCRAPE
            ):
                bad_tracker[tracker] = time.time()
        scrape.bad_tracker = bad_tracker
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


def normalize_name(name):
    name = name.replace('\r\n', ' ')
    name = name.replace('\n', '')
    name = name.replace('\r', '')
    return name


def normalize_search_archive(query):
    return " ".join(re.sub("[^\w]", " ",  query).split()).lower()

def normalize_ip_archive(ip):
    try:
        ip = netaddr.IPAddress(ip)
        if ip.version == 4:
            return ip.format()
        elif ip.version == 6:
           net = netaddr.IPNetwork("%s/64" % ip)
           return net.network.format()
        else:
            return ""
    except netaddr.AddrFormatError:
        return ""


def dmca_ban(hash):
    public_db = getdb()
    ban_db = getdb("torrents_ban")
    db = getdb("torrents")
    db.update(
        {'_id': Binary(hash)},
        {"$set": {'status': 2}},
        upsert=True
    )
    results = public_db.find({"_id": Binary(hash)}, {'_id': False})
    if results.count() == 1:
        obj = results[0]
        obj["dmca_deleted"] = time.time()
        ban_db.update({'_id': Binary(hash)}, obj, upsert=True)
        public_db.remove({"_id": Binary(hash)})
        obj['_id'] = hash
        return obj


def dmca_unban(hash):
    public_db = getdb()
    ban_db = getdb("torrents_ban")
    db = getdb("torrents")
    results = db.find({"_id": Binary(hash)})
    # hash is not banned
    if results.count() == 0:
        return
    else:
        status = results[0]
    # hash is not banned
    if status['status'] != 2:
        return
    results = ban_db.find({"_id": Binary(hash)}, {'_id': False})
    if results.count() == 1:
        obj = results[0]
        del obj["dmca_deleted"]
        public_db.update({'_id': Binary(hash)}, obj, upsert=True)
        ban_db.remove({"_id": Binary(hash)})
        db.update(
            {'_id': Binary(hash)},
            {"$set": {'status': 1}},
            upsert=True
        )
        return obj
    else:
        db.update(
            {'_id': Binary(hash)},
            {"$set": {'status': 0}},
            upsert=True
        )

import models
