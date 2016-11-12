# -*- coding: utf-8 -*-
from .settings import settings
from django.http import Http404, StreamingHttpResponse, HttpResponse, HttpResponseNotModified
from django.shortcuts import render, redirect
from django.core.exceptions import PermissionDenied
from django.views.decorators.http import require_http_methods, require_safe
from wsgiref.util import FileWrapper

from btdht.utils import bencode, bdecode
import os
import sys
import json
import time
import collections
import hashlib

from .utils import context, getdb, format_date, scrape, render_json, normalize_name
from .forms import SearchForm
from .models import Torrent, UserPref
import const


@require_http_methods(["GET", "HEAD", "POST"])
def index(request, page=1, query=None, order_by=const.ORDER_BY_SCORE, asc='1', category=0):
    torrents = None
    page = int(page)
    category = int(category if category is not None else 0)
    timezone = request.COOKIES.get('timezone', 'UTC')
    if page < 1:
        return redirect(
            "btdht_search:index_query",
            page=1,
            query=query,
            order_by=order_by,
            asc=asc,
            category=category
        )
    request.session["query"] = query
    request.session["category"] = category
    if request.method in {"GET", "HEAD"}:
        form = SearchForm(initial={'query': query, 'category': category})
    elif request.method == "POST":
        form = SearchForm(request.POST, initial={'query': query, 'category': category})
        if form.is_valid():
            return redirect(
                "btdht_search:index_query",
                page=1,
                query=form.cleaned_data["query"],
                order_by=const.ORDER_BY_SCORE,
                asc='1',
                category=form.cleaned_data["category"]
            )
    if query is not None:
        torrents = Torrent.search(
            query,
            page=page,
            order_by=order_by,
            asc=(asc == '1'),
            category=category,
            timezone=timezone
        )
        if page > torrents.last_page:
            return redirect(
                "btdht_search:index_query",
                page=torrents.last_page,
                query=query,
                order_by=order_by,
                asc=asc,
                category=category
            )
    return render(
        request,
        "btdht_search/index.html",
        context({'form': form, 'torrents': torrents, 'query': query, 'category': category})
    )


@require_safe
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


@require_safe
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


@require_http_methods(["GET", "HEAD", "POST"])
def info_torrent(request, hex_hash, name):
    timezone = request.COOKIES.get('timezone', 'UTC')
    try:
        torrent = Torrent(hex_hash.decode("hex"), timezone=timezone)
    except ValueError:
        raise Http404()
    if name != torrent.name and name != normalize_name(torrent.name):
        raise Http404()
    if request.method == "POST":
        if 'scrape' in request.POST:
            torrent.scrape()
            return redirect("btdht_search:info_torrent", hex_hash, name)
    if torrent.last_scrape == 0:
        torrent.scrape()
    return render(request, "btdht_search/torrent.html", context({'torrent': torrent}))


@require_safe
def api_info_torrent(request, hex_hash):
    try:
        torrent = Torrent(hex_hash.decode("hex"))
    except ValueError:
        raise Http404()
    return render_json(torrent.data(request))


@require_safe
def recent(request, page=1):
    page = int(page)
    timezone = request.COOKIES.get('timezone', 'UTC')
    if page < 1:
        return redirect("btdht_search:recent", 1)
    torrents = Torrent.recent(page, settings.BTDHT_RECENT_MAX, timezone=timezone)
    if page > torrents.last_page:
        return redirect("btdht_search:recent", torrents.last_page)
    request.session["query"] = None
    return render(
        request,
        "btdht_search/recent.html",
        context({
            'torrents': torrents,
        })
    )


@require_safe
def api_recent(request, page=1):
    page = int(page)
    if page < 1:
        raise Http404()
    torrents = Torrent.recent(page)
    if page > torrents.last_page:
        raise Http404()
    return render_json(torrents.data(request))


@require_safe
def stats(request):
    db = getdb("torrents_stats")
    timezone = request.COOKIES.get('timezone', 'UTC')
    results = db.find({'_id': {'$gte': time.time() - 30 * 24 * 3600}}).sort([('_id', 1)])
    torrent_indexed = []
    categories = collections.defaultdict(list)
    times = []
    bad_tracker = scrape.bad_tracker.keys()
    bad_tracker.extend(settings.BTDHT_TRACKERS_NO_SCRAPE)
    good_tracker = [tracker for tracker in settings.BTDHT_TRACKERS if tracker not in bad_tracker]
    for result in results:
        x = format_date(result['_id'], '%Y-%m-%d %H:%M:%S %z', timezone=timezone)
        times.append(x)
        torrent_indexed.append({'x': x, 'y': result["torrent_indexed"]})
        for cat in const.categories:
            y = result.get(cat, 0)
            if y > 0:
                categories[cat].append({'x': x, 'y': y})
    colors = [(int(c[0:2], 16), int(c[2:4], 16), int(c[4:6], 16)) for c in const.categories_colors]
    json_cat = []
    for i, cat in enumerate(const.categories):
        json_cat.append((cat, json.dumps(categories[cat]), colors[i]))
    return render(
        request,
        "btdht_search/stats.html",
        context({
            'torrent_indexed': json.dumps(torrent_indexed),
            'categories': json_cat,
            'times': json.dumps(times),
            'good_tracker': good_tracker,
            'bad_tracker': bad_tracker
        })
    )


@require_safe
def api(request):
    if request.user.is_authenticated():
        user_pref = UserPref.objects.get_or_create(user=request.user)[0]
    else:
        user_pref = None
    return render(request, "btdht_search/api.html", context({'user_pref': user_pref}))


@require_safe
def about(request):
    if request.method not in {"GET", "HEAD"}:
        return HttpResponse(status=405)
    return render(request, "btdht_search/about.html", context({}))


@require_safe
def sitemap(request, file=None):
    if settings.BTDHT_SITEMAP_DIR is None:
        raise Http404()
    if file is not None:
        if '/' in file:
            raise Http404()
        file_path = os.path.join(settings.BTDHT_SITEMAP_DIR, file)
    else:
        file_path = os.path.join(settings.BTDHT_SITEMAP_DIR, "index.xml.gz")
    if not os.path.isfile(file_path):
        raise Http404()
    last_modified = format_date(os.stat(file_path).st_mtime, "%a, %d %b %Y %H:%M:%S GMT")
    etag = '"%s"' % hashlib.md5(last_modified).hexdigest()

    sys.stderr.write("%s == %s\n" % (request.META.get('HTTP_IF_NONE_MATCH'), etag))
    if (
        request.META.get("HTTP_IF_MODIFIED_SINCE") == last_modified or
        request.META.get('HTTP_IF_NONE_MATCH') == etag
    ):
        response = HttpResponseNotModified()
        response['Last-Modified'] = last_modified
        response['Etag'] = etag
        return response
    response = StreamingHttpResponse(
        FileWrapper(open(file_path, 'rb'), 8192),
        content_type="application/xml"
    )
    response['Content-Length'] = os.path.getsize(file_path)
    response['Content-Encoding'] = 'gzip'
    response['Last-Modified'] = last_modified
    response['Etag'] = etag
    return response


@require_safe
def robots_txt(request):
    return render(request, "btdht_search/robots.txt", content_type="text/plain")
