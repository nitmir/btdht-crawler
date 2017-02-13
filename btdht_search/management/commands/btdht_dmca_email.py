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
import re
import email
import urlparse
try:
    import imaplib2
except ImportError:
    imaplib2 = None
import binascii
from django.urls import reverse

from ...settings import settings
from ...utils import dmca_ban
from ...urls import urlpatterns

# extract torrent info pattern from its url pattern definition
info_torrent = [url for url in urlpatterns if url.name == 'info_torrent'][0]
info_torrent_pattern = info_torrent.regex.pattern
if info_torrent_pattern.startswith("^"):
    info_torrent_pattern = info_torrent_pattern[1:]
if info_torrent_pattern.endswith("$"):
    info_torrent_pattern = info_torrent_pattern[:-1]

# url starts always with http or https :// a domain name (no /) and a /, then,
# we only match url corresponding to the torrent info pattern
URL_RE = re.compile('(http?s://[^/]*/%s)' % info_torrent_pattern)


class Break(Exception):
    pass


def decode_dkim_header(data):
    result = {}
    for field in data.replace('\r\n ', '').split(';'):
        (key, value) = field.strip().split('=', 1)
        result[key.strip()] = value.strip()
    result['h'] = [h.lower() for h in result.get('h', "").split(':')]
    return result


class DMCA(object):

    imap = None

    def connect(self):
        self.imap = imaplib2.IMAP4_SSL(settings.BTDHT_DMCA_EMAIL_SERVER)
        self.imap.login(settings.BTDHT_DMCA_EMAIL_USERNAME, settings.BTDHT_DMCA_EMAIL_PASSWORD)
        self.imap.select(settings.BTDHT_DMCA_EMAIL_MAILBOX)

    def fetch(self, id_):
        status, response = self.imap.fetch(id_, '(RFC822)')
        if status == 'OK':
            return email.message_from_string(response[0][1])

    def archive(self, id_):
        status, data = self.imap.copy(id_, settings.BTDHT_DMCA_EMAIL_ARCHIVE)
        if status == 'OK' and data[0].endswith('Copy completed.'):
            self.imap.store(id_, '+FLAGS', '\\Deleted')

    def get_url_hash(self, id_):
        urls = set()
        hashes = set()
        mail = self.fetch(id_)
        if (
            settings.BTDHT_DMCA_EMAIL_DKIM and
            (
                not mail['Authentication-Results'] or
                'dkim=pass' not in mail['Authentication-Results']
            )
        ):
            sys.stderr.write("dkim failed, ignore mail id %s\n" % id_)
            return (None, None)
        try:
            for header_name, header_values in settings.BTDHT_DMCA_EMAIL_ALLOWED_HEADERS.items():
                for header_value in header_values:
                    if mail[header_name] == header_value:
                        if settings.BTDHT_DMCA_EMAIL_DKIM:
                            dkim = decode_dkim_header(mail['DKIM-Signature'])
                            if header_name.lower() in dkim['h']:
                                raise Break()
                        else:
                            raise Break()
        except Break:
            pass
        else:
            sys.stderr.write(
                "no allowed header found or not protected with dkim, ignore mail id %s\n" % id_
            )
            return (None, None)
        to_process = [mail]
        parts = []
        while to_process:
            part = to_process.pop()
            if part.is_multipart():
                for p in part.get_payload():
                    to_process.append(p)
            else:
                parts.append(part)
        for part in parts:
            data = part.get_payload(decode=True)
            for url in URL_RE.findall(data):
                hash = url[1]
                url = url[0]
                if urlparse.urlparse(url).netloc in settings.ALLOWED_HOSTS:
                    urls.add(url)
                    hashes.add(hash)
        return (urls, hashes)

    def ban(self):
        try:
            for id_ in self.get_all_mail_id():
                print("Processing mail id %s:" % id_)
                hashes = self.get_url_hash(id_)[1]
                if hashes is not None:
                    for hash in hashes:
                        sys.stdout.write(" * %s" % hash)
                        bin_hash = binascii.a2b_hex(hash)
                        obj = dmca_ban(bin_hash)
                        if obj:
                            sys.stdout.write((u": %s" % obj['name']).encode("utf-8"))
                        print("")
                    self.archive(id_)
        finally:
            self.imap.expunge()

    def get_all_mail_id(self):
        status, data = self.imap.search('UTF-8', 'ALL')
        return data[0].split()

    def get_all_urls(self):
        urls = set()
        for id_ in self.get_all_mail_id():
            urls_ = self.get_url_hash(id_)[0]
            if urls_ is not None:
                urls = urls | urls_
        return urls

    def get_all_hash(self, urls=None):
        hashes = set()
        for id_ in self.get_all_mail_id():
            hashes_ = self.get_url_hash(id_)[1]
            if hashes_ is not None:
                hashes = hashes | hashes_
        return hashes


class Command(BaseCommand):
    args = ''
    help = u"Parse email dmca request and automatically ban torrents"

    sitemap_torrents_re = re.compile("^[0-9]+\.xml\.gz$")

    def add_arguments(self, parser):
        parser.add_argument(
            '--list-new-hash',
            action='store_true',
            default=False,
            dest='list_new_hash',
            help="list new hash to ban"
        )
        parser.add_argument(
            '--list-new-urls',
            action='store_true',
            default=False,
            dest='list_new_urls',
            help="list new urls to ban"
        )
        parser.add_argument(
            '--ban-new',
            action='store_true',
            default=False,
            dest='ban_new',
            help="ban new urls"
        )

    def handle(self, *args, **options):
        d = DMCA()
        d.connect()
        try:
            if options["list_new_hash"]:
                for hash in d.get_all_hash():
                    print(hash)
            if options['list_new_urls']:
                for url in d.get_all_urls():
                    print(url)
            if options['ban_new']:
                d.ban()
        finally:
            d.imap.logout()
