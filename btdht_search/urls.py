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
"""urls for the app"""
from django.conf.urls import url

from .utils import require_login, token_auth

import views

app_name = "btdht_search"

urlpatterns = [
    url('^$', require_login(views.index), name='index'),
    url(
        '^download/(?P<hex_hash>[0-9A-Fa-f]{40})/(?P<name>.*)\.torrent$',
        require_login(views.download_torrent),
        name='download_torrent'
    ),
    url(
        'api/auth/^(?P<token>[0-9A-Fa-f]{32})/download/(?P<hex_hash>[0-9A-Fa-f]{40})/(?P<name>.*)\.torrent$',
        token_auth(views.download_torrent),
        name='download_torrent'
    ),
    url(
        '^torrent/(?P<hex_hash>[0-9A-Fa-f]{40})/(?P<name>.*)?$',
        require_login(views.info_torrent),
        name='info_torrent'
    ),
    url(
        '^api/torrent/(?P<hex_hash>[0-9A-Fa-f]{40}).json$',
        require_login(views.api_info_torrent),
        name='api_info_torrent'
    ),
    url(
        '^api/auth/(?P<token>[0-9A-Fa-f]{32})/torrent/(?P<hex_hash>[0-9A-Fa-f]{40}).json$',
        token_auth(views.api_info_torrent),
        name='api_info_torrent_token'
    ),
    url(
        (
            '^search/(?P<query>.*)/(?P<page>[0-9]+)/(?P<order_by>[1-7])/'
            '(?P<asc>[0-1])/(?P<category>[0-7])$'
        ),
        require_login(views.index),
        name='index_query'
    ),
    url(
        (
            '^api/search/(?P<query>.*)/(?P<page>[0-9]+)/(?P<order_by>[1-7])/'
            '(?P<asc>[0-1])/(?P<category>[0-7]).json'
        ),
        require_login(views.api_search),
        name='api_search'
    ),
    url(
        (
            '^api/auth/(?P<token>[0-9A-Fa-f]{32})/search/(?P<query>.*)/(?P<page>[0-9]+)/',
            '(?P<order_by>[1-7])/(?P<asc>[0-1])/(?P<category>[0-7]).json'
        ),
        token_auth(views.api_search),
        name='api_search_token'
    ),
    url('^autocomplete$', require_login(views.autocomplete), name="autocomplete"),
    url('^recent$', require_login(views.recent), name='recent_index'),
    url('^api/recent.json$', require_login(views.api_recent), name='api_recent_index'),
    url('^recent/(?P<page>[0-9]+)$', require_login(views.recent), name='recent'),
    url('^api/recent/(?P<page>[0-9]+).json', require_login(views.api_recent), name='api_recent'),
    url(
        '^api/auth/(?P<token>[0-9A-Fa-f]{32})/recent.json$',
        token_auth(views.api_recent),
        name='api_recent_index_token'
    ),
    url(
        '^api/auth/(?P<token>[0-9A-Fa-f]{32})/recent/(?P<page>[0-9]+).json',
        token_auth(views.api_recent),
        name='api_recent_token'
    ),
    url('^stats$', require_login(views.stats), name='stats'),
    url('^api$', require_login(views.api), name='api'),
    url('^about$', require_login(views.about), name='about'),
    url("^sitemap.xml.gz", require_login(views.sitemap), name="sitemap_index"),
    url("^sitemap/(?P<file>[0-9A-Za-z\.]+)", require_login(views.sitemap), name="sitemap"),
    url("^robots.txt$", require_login(views.robots_txt), name="robots.txt"),
    url("^dmca$", require_login(views.dmca), name="dmca"),
    url("^legal$", require_login(views.legal), name="legal"),
]
