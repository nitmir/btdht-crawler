#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import gzip
#import MySQLdb
try:
    import mysql.connector as MySQLdb
except ImportError:
    import MySQLdb
import config
import urllib
import requests
import pyaria2.pyaria2 as pyaria2
from dht import utils
import hashlib
import time
import chardet
import pycurl
import socket
import scraper
import xmlrpc.client
import progressbar
import concurrent.futures
from threading import Thread
import queue

from replication import Replicator

socket.setdefaulttimeout(10)
last_get = time.time() - 24 * 3600

def widget(what=""):
    padding = 30
    return [progressbar.ETA(), ' ', progressbar.Bar('='), ' ', progressbar.SimpleProgress(), ' ' if what else "", what]
def cancelable(f):
    def func(*arg, **kwargs):
        try:
            return f(*arg, **kwargs)
        except KeyboardInterrupt:
            print("\r")
    return func

def last_timestamp(self):
    global last_get
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(created))    

def get_to_download(aria2):
    hashs=[]
    for info in aria2.tellActive(keys=["infoHash"]) + aria2.tellWaiting(0, 1000000000, keys=["infoHash"]):
        if 'infoHash' in info:
            hashs.append(info['infoHash'])
    return hashs


def init_scrape_table(db=None):
    if db is None:
        db = MySQLdb.connect(**config.mysql)
    cur = db.cursor()
    cur.execute("SET max_heap_table_size = 1024*1024*1024")
    #cur.execute("TRUNCATE TABLE scrape")
    cur.execute("INSERT scrape SELECT id,NULL,0,0,0 FROM torrents WHERE name is not null and id not in (SELECT id FROM scrape)")
    db.commit()
    cur.close()
# INSERT scrape SELECT id,NULL,0,0,0 FROM torrents WHERE name is not null and id not in (SELECT torrent_id FROM scrape)
def scrape(db):
    cur = db.cursor()
    query = "SELECT hash FROM torrents WHERE name IS NOT NULL AND (scrape_date IS NULL OR scrape_date <= DATE_SUB(NOW(), INTERVAL 30 MINUTE)) ORDER BY scrape_date ASC"
    #query = "SELECT tr.id, tr.hash FROM torrents AS tr, scrape WHERE scrape.id = tr.id AND (scrape.scrape_date IS NULL OR scrape.scrape_date <= DATE_SUB(NOW(), INTERVAL 30 MINUTE))"
    cur.execute(query)
    count=0
    #hashs = [(r[1], r[0]) for r in cur]
    hashs = [r[0] for r in cur]
    count=len(hashs)
    qhashs = queue.Queue()
    i=0
    n_count = 0

    if count <= 0:
        return
    while hashs[i:i+74]:
        qhashs.put(hashs[i:i+74])
        i=min(i + 74, count)

    pbar = progressbar.ProgressBar(widgets=widget("torrents scraped"), maxval=count).start()    
    def scrape_thread(qhashs):
        nonlocal n_count, count, pbar
        db = MySQLdb.connect(**config.mysql)
        cur = db.cursor()
        last_commit = time.time()
        try:
            while True:
                #l = dict(qhashs.get(timeout=0))
                l = qhashs.get(timeout=0)
                for hash, info in scraper.scrape("udp://open.demonii.com:1337/announce", l).items():
                    cur.execute("UPDATE torrents SET scrape_date=NOW(), seeders=%s, leechers=%s, downloads_count=%s WHERE hash=%s", (info['seeds'], info['peers'], info['complete'], hash))
                    #cur.execute("UPDATE scrape SET scrape_date=NOW(), seeders=%s, leechers=%s, downloads_count=%s WHERE id=%s", (info['seeds'], info['peers'], info['complete'], l[hash]))
                if time.time() - last_commit > 30:
                    db.commit()
                    last_commit = time.time()
                n_count+=70
                pbar.update(min(n_count, count))
        except queue.Empty:
            pass
        except socket.timeout:
            pass
        finally:
            db.commit()
            cur.close()
            db.close()

    try:
        threads = []
        for i in range(0, 20):
            t = Thread(target=scrape_thread, args=(qhashs,))
            t.daemon = True
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        pbar.finish()
        #print("Update scraped values")
        #cur.execute("UPDATE LOW_PRIORITY scrape, torrents SET torrents.scrape_date = scrape.scrape_date, torrents.seeders = scrape.seeders, torrents.leechers = scrape.leechers, torrents.downloads_count = scrape.downloads_count WHERE torrents.id = scrape.id AND torrents.scrape_date != scrape.scrape_date;")
    finally:
        db.commit()
        cur.close()

    #i=0
    #pbar = progressbar.ProgressBar(widgets=widget("torrents scraped"), maxval=count).start()    
    #try:
    #    while hashs[i:i+74]:
    #        for hash, info in scraper.scrape("udp://open.demonii.com:1337/announce", hashs[i:i+74]).items():
                #print(hash)
    #            cur.execute("UPDATE torrents SET scrape_date=NOW(), seeders=%s, leechers=%s, downloads_count=%s WHERE hash=%s", (info['seeds'], info['peers'], info['complete'], hash))
    #        db.commit()
            #cur.execute(query)
            #hashs = [r[0] for r in cur]
            #count+=len(hashs)
    #        i=min(i + 74, count)
    #        pbar.update(i)
    #finally:
    #    db.commit();
    #    pbar.finish()


downloading = []
def init_aria():
    global aria2, downloading
    aria2 = pyaria2.PyAria2(port=15859, session=os.path.join(os.path.realpath("./"),"aria2.session"))
    time.sleep(1)
    options = aria2.getGlobalOption()
    options["bt-save-metadata"]="true"
    options["bt-metadata-only"]="true"
    #options["bt-stop-timeout"]=str(600)
    options["bt-stop-timeout"]=str(0)
    options["max-concurrent-downloads"]=1000000000
    options['dir']=os.path.realpath("torrents/")
    aria2.changeGlobalOption(options)


    downloading = get_to_download(aria2)


def get_hash(db):
    #db = MySQLdb.connect(**config.mysql)          
    files = os.listdir("torrents/")
    hashs = [h[:-8] for h in files if h.endswith(".torrent")]
    cur = db.cursor()
    cur.execute("SELECT hash, id FROM torrents WHERE name IS NULL AND (dht_last_get >= DATE_SUB(NOW(), INTERVAL 1 HOUR) OR dht_last_announce >= DATE_SUB(NOW(), INTERVAL 1 HOUR) OR %s)"  % " OR ".join("hash=%s" for hash in hashs), tuple(hashs))
    #t = last_timestamp()
    #cur.execute("SELECT hash FROM torrents WHERE name IS NULL AND (dht_last_get >= %s OR dht_last_announce >= %s)", (t, t))
    ret = [(r[0], r[1]) for r in cur]
    new_hash = set(hashs).difference(ret)
    for hash in new_hash:
        cur.execute("INSERT INTO torrents (hash) VALUES (%s) ON DUPLICATE KEY UPDATE hash=hash",(hash,))
    if new_hash:
        db.commit()
    cur.close()
    #db.close()
    return ret


def clean(db, hashs=None):
    global aria2, downloading
    cur = db.cursor()
    print("Cleanning old db hash")
    query = "DELETE FROM torrents WHERE name IS NULL AND (dht_last_get < DATE_SUB(NOW(), INTERVAL 24 HOUR) OR dht_last_get IS NULL) AND (dht_last_announce < DATE_SUB(NOW(), INTERVAL 24 HOUR) OR dht_last_announce IS NULL)"
    cur.execute(query)
    db.commit()
    if hashs is None:
        hid = get_hash(db)
        if hid:
            hashs = set(list(zip(*hid))[0])
        else:
            hashs = []
    if not hashs:
        return
    downloading = get_to_download(aria2)
    active = aria2.tellActive(keys=["infoHash", "gid"])
    progress = progressbar.ProgressBar(widgets=widget("hash removed from aria2"))
    for info in progress(active):
        if 'infoHash' in info:
            if not info['infoHash'] in hashs:
                gid = info["gid"]
                try:
                    aria2.remove(gid)
                    aria2.removeDownloadResult(gid)
                except xmlrpc.client.Fault:
                    pass
                downloading.remove(info['infoHash'])
                #print("%s too old, removed" % info['infoHash'])
    add_to_aria(db)


def fetch_torrent(db):
    cur = db.cursor()
    cur.execute("SELECT hash, id FROM torrents WHERE name IS NULL AND (torcache_notfound=%s OR torcache_notfound IS NULL)", (False,))
    hashs = queue.Queue()
    [hashs.put((r[0].lower(), r[1])) for r in cur]
    notfound = set()
    notfound_nb = 0
    found_nb = 0
    cur = db.cursor()
    count = hashs.qsize()
    pbar = progressbar.ProgressBar(widgets=widget("torrents fetched"), maxval=count).start()
    c_count = 0
    def load_url(db, cur, hash, id):
        nonlocal c_count, notfound, notfound_nb, found_nb
        if os.path.isfile("torrents/%s.torrent" % hash):
            update_db(db, hashs=[(hash, id)], quiet=True)
        else:
            try:
                url = "http://torcache.net/torrent/%s.torrent" % hash.upper()
                response = urllib.request.urlopen(url)
                torrentz = response.read()
                if torrentz:
                    try:
                        bi = io.BytesIO(torrentz)
                        torrent = gzip.GzipFile(fileobj=bi, mode="rb").read()
                        if torrent:
                            open("torrents/%s.torrent" % hash, 'bw+').write(torrent)
                            #print("Found %s on torcache" % hash)
                            update_db(db, hashs=[(hash, id)], torcache=True, quiet=True)
                            Replicator.send_torrent([(hash, url)])
                            found_nb+=1
                        else:
                            print("Got empty response from torcache %s" % url)
                            notfound_nb+=1
                    except (gzip.zlib.error, OSError) as e:
                        print("%r" % e)
                        notfound_nb+=1
                else:
                    print("Got empty response from torcache %s" % url)
                    notfound_nb+=1
            except urllib.request.HTTPError:
                #print("Not Found %s, adding for download" % hash)
                cur.execute("UPDATE torrents SET torcache_notfound=%s WHERE hash=%s", (True, hash))
                notfound.add(hash)
                notfound_nb+=1
            except EOFError as e:
                print("Error on %s: %r" % (hash, e))
                notfound_nb+=1
        c_count+=1
        pbar.update(c_count)

    def process_hashs(qhashs):
        db = MySQLdb.connect(**config.mysql)
        cur = db.cursor()
        try:
            while True:
                hash, id = qhashs.get(timeout=0)
                load_url(db, cur, hash, id)
        except queue.Empty:
            pass
        except socket.timeout:
            print("socket timeout http://torcache.net/torrent/%s.torrent" % hash.upper())
        except ConnectionResetError:
            print("ConnectionResetError http://torcache.net/torrent/%s.torrent" % hash.upper())
        finally:
            db.commit()
            db.close()
    try:
        threads = []
        for i in range(0, 20):
            t = Thread(target=process_hashs, args=(hashs,))
            t.daemon = True
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        #for hash in hashs:
        #    load_url(db, cur, hash)
    finally:
        pbar.finish()
        db.commit()
    print("%s found, %s not found" % (found_nb, notfound_nb))
    return notfound
            
def get_torrent(db, hash, base_path="torrents"):
    global downloading, failed
    if hash in failed:
        return None
    if os.path.isfile("%s/%s.torrent" % (base_path, hash)):
        torrent = open("%s/%s.torrent" % (base_path, hash), 'rb').read()
        try:
            return utils.bdecode(torrent)
        except utils.BcodeError as e:
            print("FAILED %s: %r" % (hash, e))
            failed.add(hash)
    else:
        return


def update_aria(db):
    to_remove = set()
    cur = db.cursor()
    stoped = aria2.tellStopped(0, 1000, keys=["infoHash", "gid"])
    if not stoped:
        return
    progress = progressbar.ProgressBar(widgets=widget("torrents updated from aria2"))
    hashs = []
    for info in progress(stoped):
        if 'infoHash' in info:
            torrent = get_torrent(db, info['infoHash'])
            if torrent:
                if update_db_torrent(db, cur, info['infoHash'], torrent, id=id, quiet=True):
                    to_remove.add(info["gid"])
            else:
                to_remove.add(info["gid"])
        else:
            print("\rNo infoHash %r" % info)
            to_remove.add(info["gid"])

    for gid in to_remove:
        try:
            aria2.removeDownloadResult(gid)
        except pyaria2.xmlrpc.client.Fault as e:
            print(str(e))


def add_to_aria(db, hashs=None):
    global downloading
    #downloading = get_to_download(aria2)
    if hashs is None:
        cur = db.cursor()
        cur.execute("SELECT hash FROM torrents WHERE torcache_notfound=%s AND name IS NULL AND (dht_last_get >= DATE_SUB(NOW(), INTERVAL 1 HOUR) OR dht_last_announce >= DATE_SUB(NOW(), INTERVAL 1 HOUR))", (True,))
        hashs = list(set(r[0].lower() for r in cur).difference(downloading))
    if not hashs:
        return hashs
    progress = progressbar.ProgressBar(widgets=widget("torrents added to aria2"))
    for h in progress(hashs):
        aria2.addUri(["magnet:?xt=urn:btih:%s" % h])
        downloading.append(h)
    return hashs
    
    
    
def format_size(i):
    if i > 1024**4:
        return "%s TB" % round(i/(1024.0**4), 2)
    elif i > 1024**3:
        return "%s GB" % round(i/(1024.0**3), 2)
    elif i > 1024**2:
        return "%s MB" % round(i/(1024.0**2), 2)
    elif i > 1024**1:
        return "%s KB" % round(i/(1024.0**1), 2)
    else:
        return "%s B" % i

def get_torrent_info(db, hash, torrent):
    if not torrent:
        return
    encoding = None
    if b'encoding' in torrent and torrent[b'encoding'].decode():
       encoding = torrent[b'encoding'].decode()
       if encoding in ['utf8 keys', 'mbcs']:
           encoding = "utf-8"
    else:
        if b'name' in torrent[b'info']:
            encoding = chardet.detect(torrent[b'info'][b'name'])['encoding']
        if not encoding:
            encoding = "utf-8"
        
        
    if b'name.utf-8' in torrent[b'info']:
        name = torrent[b'info'][b'name.utf-8'].decode("utf-8")
    else:
        try:
            name = torrent[b'info'][b'name'].decode(encoding)
        except UnicodeDecodeError as e:
            encoding = chardet.detect(torrent[b'info'][b'name'])['encoding']
            if not encoding:
                raise e
            name = torrent[b'info'][b'name'].decode(encoding, 'ignore')
    try:
        created = int(torrent.get(b'creation date', int(time.time())))
    except ValueError as e:
        created = int(time.time())

    files_nb = len(torrent[b'info'][b'files']) if b'files' in torrent[b'info'] else 1
    size = sum([file[b'length'] for file in torrent[b'info'][b'files']]) if b'files' in torrent[b'info'] else torrent[b'info'][b'length']
    ppath = b"path"
    if b'files' in torrent[b'info']:
        description=name + "\n\n"
        description+="<table>\n"
        files = []
        for i in range(len(torrent[b'info'][b'files'])):
            file = torrent[b'info'][b'files'][i]
            if b'path.utf-8' in file:
                ppath = b'path.utf-8'
                encoding = 'utf-8'
            for j in range(len(file[ppath])):
                p = file[ppath][j]
                if isinstance(p, int):
                    file[ppath][j] = str(p).encode()
                elif not isinstance(p, bytes):
                    raise ValueError("path element sould not be of type %s" % type(p).__name__)
            files.append((os.path.join(*file[ppath]), file))
        files.sort(key=lambda x:x[0])
        for (_, file) in files:
            try:
                description+="<tr><td>%s</td><td>%s</td></tr>\n" % (os.path.join(*file[ppath]).decode(encoding), format_size(file[b'length']))
            except UnicodeDecodeError:
                encoding = chardet.detect(os.path.join(*file[ppath]))['encoding']
                if encoding is None:
                     encoding = "utf-8"
                description+="<tr><td>%s</td><td>%s</td></tr>\n" % (os.path.join(*file[ppath]).decode(encoding, 'ignore'), format_size(file[b'length']))
        description+="</table>"
    else:
        description=""
    
    return (name, created, files_nb, size, description)


failed=set()
def update_db_torrent(db, cur, hash, torrent, id=None, torcache=None, quiet=False):
    global failed
    real_hash = hashlib.sha1(utils.bencode(torrent[b'info'])).hexdigest()
    if real_hash == hash:
        try:
            if not quiet:
                print("\rdoing %s" % hash)
            infos = get_torrent_info(db, hash, torrent)
            if not infos:
                return
            (name, created, files_nb, size, description) = infos
            #try:
            #    cur.execute("INSERT INTO torrents (name, description, size, files_count, created_at, visible_status, hash) VALUES (%s,%s,%s,%s,%s,0,%s)", (name, description, size, files_nb, time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(created)), hash))
            #except MySQLdb.IntegrityError:
            if torcache is not None:
                cur.execute("UPDATE torrents SET name=%s, description=%s, size=%s, files_count=%s, created_at=%s, visible_status=0, torcache=%s WHERE hash=%s", (name, description, size, files_nb, time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(created)), torcache, hash))
            else:
                cur.execute("UPDATE torrents SET name=%s, description=%s, size=%s, files_count=%s, created_at=%s, visible_status=0 WHERE hash=%s", (name, description, size, files_nb, time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(created)), hash))
            #cur.execute("INSERT INTO scrape (id) VALUES (%s) ON DUPLICATE KEY UPDATE id=id", (id,))
            #os.rename("torrents/%s.torrent" % hash, "torrents_done/%s.torrent" % hash)
            if not quiet:
                print("\r  done %s" % name)
            return True
        except (KeyError, UnicodeDecodeError, LookupError, ValueError) as e:
            print("\r: %r" % e)
            failed.add(hash)
            return False
    else:
        print("\rWrong torrent hash %s != %s" % (hash, real_hash))
        try:
            cur.execute("UPDATE torrents SET hash=%s WHERE hash=%s", (real_hash, hash))
        except MySQLdb.errors.IntegrityError:
            cur.execute("DELETE FROM torrents WHERE hash=%s", (hash,))
        os.rename("torrents/%s.torrent" % hash, "torrents/%s.torrent" % real_hash)
        #update_db_torrent(db, cur, real_hash, torrent, torcache=torcache)


def update_db(db, hashs=None, torcache=None, quiet=False):
    global failed
    cur = db.cursor()
    if hashs is None:
        progress = progressbar.ProgressBar(widgets=widget("torrents"))
        quiet=True
        hashs = get_hash(db)
        if not hashs:
            return
        hashs = progress(hashs)
        print("Update db")
    if not hashs:
        return
    try:
        for hash, id in hashs:
            if os.path.isfile("torrents/%s.torrent" % hash) and not hash in failed:
                t = get_torrent(db, hash)
                if t is None:
                    continue
                update_db_torrent(db, cur, hash, t, id=id, torcache=torcache, quiet=quiet)
    finally:
        db.commit()


def clean_files(db=None):
    if db is None:
        db = MySQLdb.connect(**config.mysql)
    files = os.listdir("torrents/")
    hashs = [h[:-8] for h in files if h.endswith(".torrent")]
    cur = db.cursor()
    i=0
    step = 500
    count = len(hashs)
    if count <= 0:
        return
    pbar = progressbar.ProgressBar(widgets=widget("files cleaned"), maxval=count).start()
    while hashs[i:i+step]:
        #print("processing %s files" % len(hashs))
        query = "SELECT hash FROM torrents WHERE name IS NOT NULL AND (%s)" % " OR ".join("hash=%s" for hash in hashs[i:i+step])
        cur.execute(query, tuple(hashs[i:i+step]))
        db_hashs = [r[0] for r in cur]
        #print("  %s entrée trouvé" % len(hashs))
        c=i
        for hash in db_hashs:
            os.rename("torrents/%s.torrent" % hash, "torrents_done/%s.torrent" % hash)
            #print("    %s renamed" % hash)
            c+=1
            pbar.update(c)
        i=min(i + step, count)
        pbar.update(i)
    pbar.finish()


def compact_torrent(db=None):
    """SUpprime les info des block du torrent"""
    if db is None:
        db = MySQLdb.connect(**config.mysql)
    files = os.listdir("torrents_done/")
    hashs = [h[:-8] for h in files if h.endswith(".torrent")]
    cur = db.cursor()
    i=0
    step=500
    archive_path = "torrents_archives/%s/" % time.strftime("%Y-%m-%d")
    try: os.mkdir(archive_path)
    except OSError as e:
        if e.errno != 17: # file exist
            raise
    count = len(hashs)
    if count <= 0:
        return
    pbar = progressbar.ProgressBar(widgets=widget("files archived"), maxval=count).start()
    while hashs[i:i+step]:
        #print("processing %s files" % len(hashs))
        query = "SELECT hash, torcache FROM torrents WHERE name IS NOT NULL AND (%s)" % " OR ".join("hash=%s" for hash in hashs[i:i+step])
        cur.execute(query, tuple(hashs[i:i+step]))
        db_hashs = dict((r[0], r[1]) for r in cur)
        #print("  %s entrée trouvé" % len(hashs))
        c=i
        for hash, torcache in db_hashs.items():
            if torcache == 1:
                torrent = get_torrent(db, hash, base_path="torrents_done")
                torrent[b'info'][b'pieces'] = b''
                with open("%s%s.torrent" % (archive_path, hash), 'wb+') as f:
                    f.write(utils.bencode(torrent))
                os.remove("torrents_done/%s.torrent" % hash)
            else:
                os.rename("torrents_done/%s.torrent" % hash, "%s%s.torrent" % (archive_path, hash))
            c+=1
            pbar.update(c)
            #print("    %s compacted" % hash)
        i=min(i + step, count)
        pbar.update(i)
    pbar.finish()
    
def loop():
    last_clean = 0
    sql_error = False
    db = MySQLdb.connect(**config.mysql)
    #init_scrape_table(db)
    update_db(db)
    add_to_aria(db)
    db.commit()
    db.close()
    ####
    # notfound = fetch_torrent(db)
    # add_to_aria(db, notfound)
    # update_aria(db)
    # feed_torcache(db)
    last_loop = 0
    loop_interval = 300
    while True:
        try:
            widgets = [progressbar.Bar('>'), ' ', progressbar.ETA(), ' ', progressbar.ReverseBar('<')]
            now = time.time()
            maxval = int(max(0, loop_interval - (now - last_loop)))
            if maxval > 0:
                print("Now spleeping until the next loop")
                pbar = progressbar.ProgressBar(widgets=widgets, maxval=maxval).start()
                for i in range(0, maxval):
                    time.sleep(1)
                    pbar.update(i+1)
                pbar.finish()
            last_loop = time.time()
            print("\n\n\nNEW LOOP")
            db = MySQLdb.connect(**config.mysql)
            notfound = fetch_torrent(db)
            if notfound:
                ret = add_to_aria(db, notfound)
            update_aria(db)
            feed_torcache(db)
            #hashs=get_hash(db)
            #print("%d hashs" % len(hashs))
            #for h in hashs:
            #    _=get_torrent(db, h)
            #update_db(db)
            #feed_torcache(db)
            if time.time() - last_clean > 60*15:
                clean(db)
                last_clean = time.time()
            scrape(db)
            clean_files(db)
            compact_torrent(db)
            db.commit()
            db.close()
            sql_error = False
        except socket.timeout:
            if sql_error:
                raise
            time.sleep(10)
            sql_error = True
        except (ConnectionRefusedError, ):
            init_aria()
            db = MySQLdb.connect(**config.mysql)
            update_db(db)
            add_to_aria(db)
            db.commit()
            db.close()
        except MySQLdb.errors.OperationalError as e:
            if sql_error:
                raise
            time.sleep(10)
            sql_error = True
        finally:
            db.close()

def upload_to_torcache(db, hash, quiet=False):
    files = {'torrent': open("torrents/%s.torrent" % hash, 'rb')}
    r = requests.post('http://torcache.net/autoupload.php', files=files)
    cur = db.cursor()
    try:
        if 'X-Torrage-Error-Msg' in r.headers:
            cur.execute("UPDATE torrents SET torcache=%s WHERE hash=%s", (False, hash))
            print("\r%s" % r.headers['X-Torrage-Error-Msg'])
        elif r.headers.get('X-Torrage-Infohash', "").lower() == hash:
            cur.execute("UPDATE torrents SET torcache=%s WHERE hash=%s", (True, hash))
            if not quiet:
                print("Uploaded %s to torcache" % hash)
            Replicator.send_torrent([(hash, ("http://torcache.net/torrent/%s.torrent" % hash.upper()))])
        elif 'X-Torrage-Infohash' in r.headers:
            print("Bad hash %s -> %s" % (hash, r.headers.get('X-Torrage-Infohash')))
            update_db(db, [hash])
    finally:
        db.commit()
    return r


def feed_torcache(db, hashs=None):
    if hashs is None:
        cur = db.cursor()
        cur.execute("SELECT hash FROM torrents WHERE name IS NOT NULL AND torcache IS NULL")
        hashs = [r[0].lower() for r in cur]
    if not hashs:
        return
    progress = progressbar.ProgressBar(widgets=widget("torrents uploaded to torcache"))
    try:
        for hash in progress(hashs):
            if os.path.isfile("torrents/%s.torrent" % hash):
                t = get_torrent(db, hash)
                upload_to_torcache(db, hash, quiet=True)
            else:
                cur.execute("UPDATE torrents SET torcache=%s WHERE hash=%s", (False, hash))
    finally:
        db.commit()

if __name__ == '__main__':
    init_aria()
    loop()
