from .settings import settings
from django.http import Http404, StreamingHttpResponse, HttpResponse
from django.shortcuts import render, redirect
from django.core.exceptions import PermissionDenied
from wsgiref.util import FileWrapper

from btdht.utils import bencode, bdecode
import os
import re
import json
from bson.binary import Binary

from .utils import context, getdb, format_date, scrape, render_json
from .forms import SearchForm
from .models import Torrent, UserPref
import const

def index(request, page=1, query=None, order_by=const.ORDER_BY_SCORE, asc='1'):
    torrents = None
    page = int(page)
    if page < 1:
        return redirect("btdht_crawler:index_query", page=1, query=query, order_by=order_by, asc=asc)
    request.session["query"] = query
    if request.method == "GET":
        form = SearchForm(initial={'query': query})
    elif request.method == "POST":
        form = SearchForm(request.POST, initial={'query': query})
        if form.is_valid():
            return redirect("btdht_crawler:index_query", page=1, query=form.cleaned_data["query"], order_by=const.ORDER_BY_SCORE, asc='1')
    if query is not None:
        torrents = Torrent.search(query, page=page, order_by=order_by, asc=(asc == '1'))
        if page > torrents.last_page:
            return redirect("btdht_crawler:index_query", page=torrents.last_page, query=query, order_by=order_by, asc=asc)
    return render(request, "btdht_crawler/index.html", context({'form': form, 'torrents': torrents, 'query': query}))


def api_search(request, page=1, query=None):
    if not query:
        return render_json([])
    page = int(page)
    if page < 1:
        raise Http404()
    torrents = Torrent.search(query, page=page)
    if page > torrents.last_page:
        raise Http404()
    return render_json(torrents.data(request))

def api_search_token(request, token, page=1, query=None):
    return api_search(request, page, query)


def download_torrent(request, hex_hash, name):
   if settings.BTDHT_HIDE_TORRENT_LINK_FROM_UNAUTH and not request.user.is_authenticated():
       raise PermissionDenied()
   try:
       torrent = Torrent(hex_hash.decode("hex"))
   except ValueError:
       raise Http404()
   if name != torrent.name:
       raise Http404()
   path = torrent.path
   if not os.path.isfile(path):
       raise Http404()
   with open(path) as f:
       data = f.read()
   torrent = bdecode(data)
   torrent['announce'] = settings.BTDHT_TRACKERS[0]
   torrent['announce-list'] = [[t] for t in settings.BTDHT_TRACKERS]
   data = bencode(torrent)

   response = HttpResponse(data, content_type="application/x-bittorrent")
   response['Content-Length'] = len(data)
   if name.endswith(".torrent"):
       torrent_name = name
   else:
       torrent_name = "%s.torrent" % name
   response['Content-Disposition'] = 'attachment; filename="%s"' % torrent_name
   return response


def info_torrent(request, hex_hash, name):
    try:
        torrent = Torrent(hex_hash.decode("hex"))
    except ValueError:
        raise Http404()
    if name != torrent.name:
        raise Http404()
    if request.method == "POST":
        if 'scrape' in request.POST:
            torrent.scrape()
            return redirect("btdht_crawler:info_torrent", hex_hash, name)
    if torrent.last_scrape == 0:
        torrent.scrape()
    return render(request, "btdht_crawler/torrent.html", context({'torrent': torrent}))

def api_info_torrent(request, hex_hash):
    try:
        torrent = Torrent(hex_hash.decode("hex"))
    except ValueError:
        raise Http404()
    return render_json(torrent.data(request))


def recent(request, page=1):
    page = int(page)
    if page < 1:
        return redirect("btdht_crawler:recent", 1)
    torrents = Torrent.recent(page, 124999)
    if page > torrents.last_page:
        return redirect("btdht_crawler:recent", torrents.last_page)
    request.session["query"] = None
    return render(
        request,
        "btdht_crawler/recent.html",
        context({
            'torrents': torrents,
        })
    )

def api_recent(request, page=1):
    page = int(page)
    if page < 1:
        raise Http404()
    torrents = Torrent.recent(page)
    if page > torrents.last_page:
        raise Http404()
    return render_json(torrents.data(request))

def stats(request):
    db = getdb("torrents_stats")
    results = db.find().sort([('_id', 1)])
    torrent_indexed = []
    hash_tracked = []
    times = []
    bad_tracker = scrape.bad_tracker.keys()
    bad_tracker.extend(settings.BTDHT_TRACKERS_NO_SCRAPE)
    good_tracker = [tracker for tracker in settings.BTDHT_TRACKERS if not tracker in bad_tracker]
    for result in results:
        times.append(format_date(result['_id'], '%Y-%m-%d %H:%M:%S +00'))
        torrent_indexed.append(result["torrent_indexed"])
        hash_tracked.append(result["hash_tracked"])
    return render(
        request,
        "btdht_crawler/stats.html",
        context({
            'hash_tracked': json.dumps(hash_tracked),
            'torrent_indexed': json.dumps(torrent_indexed),
            'times': json.dumps(times),
            'good_tracker': good_tracker,
            'bad_tracker': bad_tracker
        })
    )


def api(request):
    if request.user.is_authenticated(): 
        user_pref = UserPref.objects.get_or_create(user=request.user)[0]
    else:
        user_pref = None
    return render(request, "btdht_crawler/api.html", context({'user_pref': user_pref}))

def about(request):
    return render(request, "btdht_crawler/about.html", context({}))
