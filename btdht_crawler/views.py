from .settings import settings
from django.http import Http404, StreamingHttpResponse, HttpResponse
from django.shortcuts import render, redirect
from wsgiref.util import FileWrapper

from btdht.utils import bencode, bdecode
import os
import re
from .utils import context, getdb
from .forms import SearchForm
from .models import Torrent

def index(request, page=1, query=None):
    torrents = None
    page = int(page)
    if page < 1:
        return redirect("btdht_crawler:index_query", 1, query)
    request.session["query"] = query
    if request.method == "GET":
        form = SearchForm(initial={'query': query})
    elif request.method == "POST":
        form = SearchForm(request.POST, initial={'query': query})
        if form.is_valid():
            return redirect("btdht_crawler:index_query", 1, form.cleaned_data["query"])
    if query is not None:
        torrents = Torrent.search(query, page=page)
        if page > torrents.last_page:
            return redirect("btdht_crawler:index_query", torrents.last_page, query)
    return render(request, "btdht_crawler/index.html", context({'form': form, 'torrents': torrents, 'query': query}))


def download_torrent(request, hex_hash, name):
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


def recent(request, page=1):
    page = int(page)
    if page < 1:
        return redirect("btdht_crawler:recent", 1)
    torrents = Torrent.recent(page)
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

