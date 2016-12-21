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

from ...utils import getdb


class Command(BaseCommand):
    args = ''
    help = u"Perform a random search (among past search) to force mongodb to keep indexes in RAM"

    sitemap_torrents_re = re.compile("^[0-9]+\.xml\.gz$")

    def handle(self, *args, **options):
        query = getdb("torrents_search").aggregate([{'$sample':{'size': 1}}]).next()['query']
        print(u"Search for %s" % query).encode("utf-8")
        results = getdb().find(
            {"$text": {"$search": query, '$language': "english"}},
            {"score": {"$meta": "textScore"}, 'name': True}
        ).sort([("score", {"$meta": "textScore"})]).limit(25)
        for result in results:
            print(u" * %s" % result['name']).encode("utf-8")
