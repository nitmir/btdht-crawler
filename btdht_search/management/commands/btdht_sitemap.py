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
# (c) 2016 Valentin Samir
from django.core.management.base import BaseCommand, CommandError
from django.core.urlresolvers import reverse

import os
import gzip
import re
from xml.sax.saxutils import escape

from ...settings import settings
from ...utils import getdb, normalize_name, format_date


MAX_SIZE = 35000


class Command(BaseCommand):
    args = ''
    help = u"Build/update the btdht sitemaps"

    sitemap_torrents_re = re.compile("^[0-9]+\.xml\.gz$")

    def add_arguments(self, parser):
        parser.add_argument(
            '--all',
            action='store_true',
            default=False,
            dest='all',
            help="Regenerate all sitemaps"
        )

    def handle(self, *args, **options):
        if settings.BTDHT_SITEMAP_DIR is None or not os.path.isdir(settings.BTDHT_SITEMAP_DIR):
            raise CommandError(
                "Please define the settings BTDHT_SITEMAP_DIR to a writable directory"
            )
        if settings.BTDHT_SITEMAP_BASEURL is None:
            raise CommandError(
                "Please define the settings BTDHT_SITEMAP_BASEURL to your site base URL, "
                "e.g. http://www.example.com"
            )
        self.gen_torrents(*args, **options)
        self.gen_static()
        self.gen_index(*args, **options)

    def gen_static(self):
        print("Generating static sitemap")
        urls = [
            (reverse("btdht_search:index"), "weekly"),
            (reverse("btdht_search:stats"), "hourly"),
            (reverse("btdht_search:api"), "weekly"),
            (reverse("btdht_search:about"), "weekly"),
            (reverse("btdht_search:dmca"), "hourly"),
            (reverse("btdht_search:recent_index"), "hourly"),
            (reverse("btdht_search:top_index"), "hourly"),
        ]
        with gzip.open(os.path.join(settings.BTDHT_SITEMAP_DIR, "static.xml.new.gz"), 'w') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
            for (url, freq) in urls:
                f.write('<url><loc>')
                f.write(escape(settings.BTDHT_SITEMAP_BASEURL))
                f.write(escape(url))
                f.write('</loc>')
                f.write('<changefreq>%s</changefreq>' % freq)
                f.write('</url>\n')
            f.write('</urlset>')
        os.rename(
            os.path.join(settings.BTDHT_SITEMAP_DIR, "static.xml.new.gz"),
            os.path.join(settings.BTDHT_SITEMAP_DIR, "static.xml.gz")
        )

    def gen_recent(self):
        return self._gen_list(
            "recent", {}, {'files': False}, [("created", -1)], "added", settings.BTDHT_RECENT_MAX,
            reverse("btdht_search:recent_index"), lambda page: reverse("btdht_search:recent", args=[page])
        )

    def gen_top(self):
        return self._gen_list(
            "top", {}, {'files': False}, [("seeds_peers", -1)], None, settings.BTDHT_RECENT_MAX,
            reverse("btdht_search:top_index"), lambda page: reverse("btdht_search:top", args=[0, page])
        )

    def _gen_list(self, name, query, proj, sort, key, limit, index, url):
        print("Generating %s sitemap" % name)
        db = getdb()
        results = db.find(
            query,
            proj
        ).sort(
            sort
        ).limit(1)
        if key:
            last_change = float(results.next()[key])
        else:
            last_change = None
        size = min(results.count(), limit)
        last_page = int(size/settings.BTDHT_PAGE_SIZE) + 1
        with gzip.open(os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.xml.new.gz" % name), 'w') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
            f.write('<url><loc>')
            f.write(escape(settings.BTDHT_SITEMAP_BASEURL))
            f.write(escape(index))
            f.write('</loc>')
            if last_change:
                f.write('<lastmod>')
                f.write(format_date(last_change, '%Y-%m-%dT%H:%M:%S+00:00'))
                f.write('</lastmod>')
            f.write('<changefreq>hourly</changefreq>')
            f.write('</url>\n')
            for page in xrange(1, last_page+1):
                f.write('<url><loc>')
                f.write(escape(settings.BTDHT_SITEMAP_BASEURL))
                f.write(escape(url(page)))
                f.write('</loc>')
                if last_change:
                    f.write('<lastmod>')
                    f.write(format_date(last_change, '%Y-%m-%dT%H:%M:%S+00:00'))
                    f.write('</lastmod>')
                f.write('<changefreq>hourly</changefreq>')
                f.write('</url>\n')
            f.write('</urlset>')
        if last_change:
            with open(os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.last_date.new" % name), 'w') as f:
                f.write("%s" % last_change)
        os.rename(
            os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.xml.new.gz" % name),
            os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.xml.gz" % name)
        )
        if last_change:
            os.rename(
                os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.last_date.new" % name),
                os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.last_date" % name)
            )

    def gen_torrents(self, *args, **options):
        sitemap_pages = [
            int(file[:-7]) for file in os.listdir(settings.BTDHT_SITEMAP_DIR)
            if self.sitemap_torrents_re.match(file)
        ]
        sitemap_pages.sort()
        if sitemap_pages and not options['all']:
            page = sitemap_pages[-1]
        else:
            page = 1
        db = getdb()
        req = db.find(
            {},
            {'_id': True, 'name': True, 'added': True}
        ).sort(
            [('added', 1)]
        ).skip(
            MAX_SIZE * (page - 1)
        ).limit(MAX_SIZE)
        while req.count(with_limit_and_skip=True) > 0:
            print("Generating sitemap %s" % page)
            with gzip.open(
                os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.xml.new.gz" % page),
                'w'
            ) as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
                for result in req:
                    f.write('<url><loc>')
                    f.write(escape(settings.BTDHT_SITEMAP_BASEURL))
                    f.write(
                        escape(
                            reverse(
                                "btdht_search:info_torrent",
                                args=[result["_id"].encode("hex"), normalize_name(result["name"])]
                            )
                        )
                    )
                    f.write('</loc><lastmod>')
                    f.write(format_date(result["added"], '%Y-%m-%dT%H:%M:%S+00:00'))
                    f.write('</lastmod><changefreq>never</changefreq>')
                    f.write('</url>\n')
                f.write('</urlset>')
            with open(
                os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.last_date.new" % page),
                'w'
            ) as f:
                f.write("%s" % result["added"])
            os.rename(
                os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.xml.new.gz" % page),
                os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.xml.gz" % page)
            )
            os.rename(
                os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.last_date.new" % page),
                os.path.join(settings.BTDHT_SITEMAP_DIR, "%s.last_date" % page)
            )

            page += 1
            req = db.find(
                {},
                {'_id': True, 'name': True, 'added': True}
            ).sort(
                [('added', 1)]
            ).skip(
                MAX_SIZE * (page - 1)
            ).limit(MAX_SIZE)

    def gen_index(self, *args, **options):
        print("Generating sitemap index")
        sitemap_pages = [
            file for file in os.listdir(settings.BTDHT_SITEMAP_DIR)
            if file.endswith(".xml.gz") and file != "index.xml.gz"
        ]
        sitemap_pages.sort()
        with gzip.open(os.path.join(settings.BTDHT_SITEMAP_DIR, "index.xml.new.gz"), 'w') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
            for file in sitemap_pages:
                f.write('<sitemap><loc>')
                f.write(
                    "%s%s" % (
                        settings.BTDHT_SITEMAP_BASEURL,
                        reverse("btdht_search:sitemap", args=[file])
                    )
                )
                f.write('</loc>')
                last_mod_path = os.path.join(
                    settings.BTDHT_SITEMAP_DIR,
                    "%s.last_date" % file[:-7]
                )
                if os.path.isfile(last_mod_path):
                    f.write('<lastmod>')
                    with open(last_mod_path) as f2:
                        last_change = float(f2.read())
                    f.write(format_date(last_change, '%Y-%m-%dT%H:%M:%S+00:00'))
                    f.write('</lastmod>')
                f.write('</sitemap>\n')
            f.write('</sitemapindex>')
        os.rename(
            os.path.join(settings.BTDHT_SITEMAP_DIR, "index.xml.new.gz"),
            os.path.join(settings.BTDHT_SITEMAP_DIR, "index.xml.gz")
        )
