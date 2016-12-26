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
"""Default values for the app's settings"""
from django.conf import settings
from django.contrib.staticfiles.templatetags.staticfiles import static


#: URL to the logo showed in the up left corner on the default templates.
BTDHT_LOGO_URL = static("btdht_search/logo_no_text.svg")
#: URL to the favicon (shortcut icon) used by the default templates. Default is a key icon.
BTDHT_FAVICON_URL = static("btdht_search/favicon.png")
#: Show the powered by footer if set to ``True``
BTDHT_SHOW_POWERED = True
#: URLs to css and javascript external components.
BTDHT_COMPONENT_URLS = {
    "bootstrap3_css": "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css",
    "bootstrap3_js": "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js",
    "fontawesome": "https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.css",
    "datatable_js": "https://cdn.datatables.net/1.10.12/js/jquery.dataTables.min.js",
    "datatable_css": "https://cdn.datatables.net/1.10.12/css/dataTables.bootstrap.min.css",
    "datatable_filesize": "https://cdn.datatables.net/plug-ins/1.10.12/sorting/file-size.js",
    "html5shiv": "https://oss.maxcdn.com/libs/html5shiv/3.7.0/html5shiv.js",
    "respond": "https://oss.maxcdn.com/libs/respond.js/1.4.2/respond.min.js",
    "jquery": "https:////code.jquery.com/jquery.min.js",
    "jquery_cookie": "https://cdnjs.cloudflare.com/ajax/libs/jquery-cookie/1.4.1/jquery.cookie.js",
    "jqueryui_css": "https://code.jquery.com/ui/1.12.0/themes/smoothness/jquery-ui.css",
    "jqueryui_js": "https://code.jquery.com/ui/1.12.1/jquery-ui.min.js",
    "chart_js": "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.3.0/Chart.bundle.js",
    "jstz": "https://cdnjs.cloudflare.com/ajax/libs/jstimezonedetect/1.0.6/jstz.js",
}


BTDHT_MONGODB = "btdht-crawler"

BTDHT_TRACKERS = [
    "udp://tracker.leechers-paradise.org:6969/announce",
    "udp://tracker.zer0day.to:1337/announce",
    "udp://tracker.coppersurfer.tk:6969/announce",

    "udp://9.rarbg.com:2710/announce",
    "udp://9.rarbg.me:2710/announce",
    "udp://9.rarbg.to:2710/announce",
    "udp://tracker.opentrackr.org:1337/announce",

    "udp://tracker.internetwarriors.net:1337/announce",
    "udp://tracker.sktorrent.net:6969/announce",
    "udp://tracker.pirateparty.gr:6969/announce",

    "udp://tracker.desu.sh:6969",

    "udp://tracker.internetwarriors.net:1337/announce",
    "udp://tracker.openbittorrent.com:80/announce",
    "udp://explodie.org:6969",

    "udp://tracker.piratepublic.com:1337/announce",

    'http://210.244.71.25:6969/announce',
    'http://210.244.71.26:6969/announce',
    'http://91.217.91.21:3218/announce',
    'http://mgtracker.org:6969/announce',
    'http://open.touki.ru/announce.php',
    'http://p4p.arenabg.ch:1337/announce',
    # 'http://p4p.arenabg.com:1337/announce',
    'http://retracker.gorcomnet.ru/announce',
    'http://tracker.dutchtracking.com/announce',
    'http://tracker.filetracker.pl:8089/announce',
    'http://tracker.grepler.com:6969/announce',
]

BTDHT_TRACKERS_NO_SCRAPE = [
    "udp://9.rarbg.com:2710/announce",
    "udp://9.rarbg.me:2710/announce",
    "udp://9.rarbg.to:2710/announce",
]
BTDHT_TORRENTS_BASE_PATH = None

BTDHT_PAGE_SIZE = 25
BTDHT_RECENT_MAX = 124999

BTDHT_SCRAPE_MIN_INTERVAL = 600
BTDHT_SCRAPE_BROWSE_INTERVAL = 3600
BTDHT_SCRAPE_INTERVAL = 3600
BTDHT_SCRAPE_RECENT = 7 * 24 * 3600  # 1 week
BTDHT_SCRAPE_TOP_NB = 100000

BTDHT_REQUIRE_AUTH = False
BTDHT_HIDE_MAGNET_FROM_UNAUTH = False
BTDHT_HIDE_TORRENT_LINK_FROM_UNAUTH = True

BTDHT_TOKEN_AUTH_BACKEND = None

BTDHT_SITEMAP_DIR = None
BTDHT_SITEMAP_BASEURL = None

BTDHT_CONTACT_EMAIL = None
BTDHT_CONTACT_MAILHIDE_URL = None

BTDHT_ADS_TEMPLATE = None

BTDHT_DMCA_EMAIL_USERNAME = None
BTDHT_DMCA_EMAIL_PASSWORD = None
BTDHT_DMCA_EMAIL_SERVER = None
BTDHT_DMCA_EMAIL_MAILBOX = "INBOX"
BTDHT_DMCA_EMAIL_ARCHIVE = "INBOX.done"
BTDHT_DMCA_EMAIL_ALLOWED_HEADERS = {}
BTDHT_DMCA_EMAIL_DKIM = True

BTDHT_LEGAL_TEMPLATE = "btdht_search/legal.html"
BTDHT_LEGAL_CAPTCHA_PROTECT = True
BTDHT_LEGAL_CREATOR = None
BTDHT_LEGAL_HOSTING_PROVIDER = None

BTDHT_LEGAL_ENABLE = True


BTDHT_SOCIAL_SHARE = ["twitter", "facebook", "google", "reddit"]

BTDHT_FACEBOOK_PAGE = None
BTDHT_TWITTER_NAME = None

GLOBALS = globals().copy()
for name, default_value in GLOBALS.items():
    # only care about parameter begining by BTDHT_
    if name.startswith("BTDHT_"):
        # get the current setting value, falling back to default_value
        value = getattr(settings, name, default_value)
        # set the setting value to its value if defined, ellse to the default_value.
        setattr(settings, name, value)


# Allow the user defined BTDHT_COMPONENT_URLS to omit not changed values
MERGED_BTDHT_COMPONENT_URLS = BTDHT_COMPONENT_URLS.copy()
MERGED_BTDHT_COMPONENT_URLS.update(settings.BTDHT_COMPONENT_URLS)
settings.BTDHT_COMPONENT_URLS = MERGED_BTDHT_COMPONENT_URLS

settings.BTDHT_LEGAL_ENABLE = settings.BTDHT_LEGAL_ENABLE and (
    (
        settings.BTDHT_LEGAL_TEMPLATE == "btdht_search/legal.html" and
        (
            settings.BTDHT_LEGAL_CREATOR or
            settings.BTDHT_LEGAL_HOSTING_PROVIDER
        )
    ) or settings.BTDHT_LEGAL_TEMPLATE
)
