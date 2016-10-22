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

import views

app_name = "btdht_crawler"

urlpatterns = [
    url('^$', views.index, name='index'),
    url('^2/(?P<page>[0-9]+)/(?P<query>.*)$', views.index, name='index_query'),
    url('^0/(?P<hex_hash>[0-9a-f]{40})/(?P<name>.*)\.torrent$', views.download_torrent, name='download_torrent'),
    url('^1/(?P<hex_hash>[0-9a-f]{40})/(?P<name>.*)$', views.info_torrent, name='info_torrent'),
    url('^recent$', views.recent, name='recent_index'),
    url('^recent/(?P<page>[0-9]+)$', views.recent, name='recent'),
]
