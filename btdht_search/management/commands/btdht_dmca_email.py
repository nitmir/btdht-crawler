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

import sys
import re
import time
import email
import urlparse
try:
    import imaplib2
except ImportError:
    imaplib2 = None
import binascii
from bson.binary import Binary

from ...settings import settings
from ...utils import getdb, dmca_ban


URL_RE = re.compile(
    """((?:[a-z][\\w-]+:(?:/{1,3}|[a-z0-9%])|www\\d{0,3}[.]|[a-z0-9.\\-]+[.][a-z]{2,4}/"""
    """)(?:[^\\s()<>]+|\\(([^\\s()<>]+|(\\([^\\s()<>]+\\)))*\\))+(?:\\(([^\\s()<>]+|(\\"""
    """([^\\s()<>]+\\)))*\\)|[^\\s`!()\\[\\]{};:'".,<>?«»""'']))"""
)

URL_HASH_RE = re.compile(".*/([0-9A-Fa-f]{40})/.*")


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

    def get_url(self, id_):
       urls = set()
       mail = self.fetch(id_)
       if (
           settings.BTDHT_DMCA_EMAIL_DKIM and
           (
               not mail['Authentication-Results'] or
               not 'dkim=pass' in mail['Authentication-Results']
           )
       ):
           sys.stderr.write("dkim failed, ignore mail id %s\n" % id_)
           return
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
           sys.stderr.write("no allowed header found or not protected with dkim, ignore mail id %s\n" % id_)
           return
       if mail.is_multipart():
           parts = mail.get_payload()
       else:
           parts = [mail]
       for part in parts:
           data = part.get_payload(decode=True)
           for url in URL_RE.findall(data):
               url = url[0]
               if urlparse.urlparse(url).netloc in settings.ALLOWED_HOSTS:
                   urls.add(url)
       return urls

    def get_hash(self, id_):
        hashes = set()
        urls = self.get_url(id_)
        if urls is not None:
            for url in urls:
                match = URL_HASH_RE.match(url)
                if match is not None:
                    hashes.add(match.group(1))
            return hashes


    def ban(self):
        try:
            for id_ in self.get_all_mail_id():
                print("Processing mail id %s:" % id_)
                hashes = self.get_hash(id_)
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
            urls_ = self.get_url(id_)
            if urls_ is not None:
                urls = urls | urls_
        return urls

    def get_all_hash(self, urls=None):
        hashes = set()
        for id_ in self.get_all_mail_id():
            hashes_ = self.get_hash(id_)
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
            help="Regenerate all sitemaps"
        )
        parser.add_argument(
            '--list-new-urls',
            action='store_true',
            default=False,
            dest='list_new_urls',
            help="Regenerate all sitemaps"
        )
        parser.add_argument(
            '--ban-new',
            action='store_true',
            default=False,
            dest='ban_new',
            help="Regenerate all sitemaps"
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


