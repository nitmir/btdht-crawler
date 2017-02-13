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
from django.core.management.base import BaseCommand

import sys
import binascii
import argparse



from ...utils import getdb
from ...settings import settings

INDEXES = {
    "torrents_data": [
        [("name", "text")], [("added", 1)], [("created", 1)], [("file_nb", 1)], [("size", 1)],
        [("categories", 1)], [("peers", -1)], [("seeds", -1)], [("last_scrape", 1)], [("complete", 1)],
        [("seeds_peers", -1)], [("seeds_peers", -1), ("seeds", -1)]
    ],
    "torrents": [[("status", 1)]],
    "torrents_ban": [[("dmca_deleted", 1)]],
    "torrents_search": [[("query", 1)], [("date", 1)]],
}


class Command(BaseCommand):
    args = ''
    help = u"Add indexes to the mongodb database"

    def handle(self, *args, **options):
        for db_name, indexes in INDEXES.items():
            print("Checking index for %s" % db_name)
            db = getdb(db_name)
            already_set = []
            for index in db.index_information().values():
                if len(index["key"]) >= 1:
                    if index["key"][0][1] != 'text':
                        already_set.append(index["key"])
                    elif len(index["key"]) == 2 and index["key"][0][1] == 'text':
                        already_set.append([(key, 'text') for key in index['weights']])
            for keys in indexes:
                if not keys in already_set:
                    sys.stdout.write(" * Adding an index on %s..." % ", ".join(name for (name, value) in keys))
                    sys.stdout.flush()
                    db.create_index(keys)
                    print("OK")
