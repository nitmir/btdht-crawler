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

import re
import binascii
import argparse

from ...utils import dmca_ban, dmca_unban


def check_hex_hash(value):
    if not re.match("[0-9A-Za-z]{40}", value):
        raise argparse.ArgumentTypeError("%r is not a valid torrent hash" % value)
    return value


class Command(BaseCommand):
    args = ''
    help = u"Allow to ban or unban torrent hashes"

    def add_arguments(self, parser):
        parser.add_argument(
            '--ban',
            dest='ban',
            help="A hash to ban",
            type=check_hex_hash
        )
        parser.add_argument(
            '--unban',
            dest='unban',
            help="A hash to unban",
            type=check_hex_hash
        )

    def handle(self, *args, **options):
        if options["ban"]:
            dmca_ban(binascii.a2b_hex(options["ban"]))
            print("%s banned" % options["ban"])
        if options["unban"]:
            dmca_unban(binascii.a2b_hex(options["unban"]))
            print("%s unbanned" % options["unban"])
