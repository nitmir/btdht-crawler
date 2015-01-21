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

def scrape(db):
    cur = db.cursor()
    query = "SELECT hash FROM torrents WHERE name IS NOT NULL AND (scrape_date IS NULL OR scrape_date <= DATE_SUB(NOW(), INTERVAL 30 MINUTE)) ORDER BY scrape_date ASC"
    cur.execute(query)
    count=0
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
        except (socket.timeout, PermissionError):
            qhashs.put(l)
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
    finally:
        db.commit()
        cur.close()

def update_torrent_file(db=None):
    if db is None:
        db = MySQLdb.connect(**config.mysql)
    hashs = get_hash(db, insert_new=True, dir_only=True)
    update_db(db, hashs, quiet=True)

def insert(new_hashq, pbar):
        db = MySQLdb.connect(**config.mysql)
        cur = db.cursor()
        try:
            while True:
                hash = new_hashq.get(timeout=0)
                if is_torcache(hash):
                    cur.execute("INSERT INTO torrents (hash,dht_last_get,torcache) VALUES (%s,NOW(),%s)",(hash, True))
                else:
                    cur.execute("INSERT INTO torrents (hash,dht_last_get) VALUES (%s,NOW())",(hash,))
                pbar.update(pbar.currval+1)
        except queue.Empty:
            pass
        finally:
            db.commit()
            cur.close()
            db.close()

def get_new_torrent():
    files = os.listdir("torrents_new/")
    if not files:
        return
    progress = progressbar.ProgressBar(widgets=widget("new torrents"))
    for f in progress(files):
        f = "torrents_new/%s" % f
        try:
            torrent = utils.bdecode(open(f, 'rb').read())    
            real_hash = hashlib.sha1(utils.bencode(torrent[b'info'])).hexdigest()
            os.rename(f, "torrents/%s.torrent" % real_hash)
        except utils.BcodeError as e:
            pass

def get_hash(db=None, insert_new=False, dir_only=False):
    if db is None:
        db = MySQLdb.connect(**config.mysql)          
    cur = db.cursor()
    #query = "SELECT hash FROM torrents WHERE name IS NULL AND (dht_last_get >= DATE_SUB(NOW(), INTERVAL 1 HOUR) OR dht_last_announce >= DATE_SUB(NOW(), INTERVAL 1 HOUR) OR %s)"  % " OR ".join("hash=%s" for hash in hashs)
    files = os.listdir("torrents/")
    hashs = [h[:-8] for h in files if h.endswith(".torrent")]
    if insert_new:
        khashs = set()
        i=0
        while hashs[i:i+50]:
            query = "SELECT hash FROM torrents WHERE (%s)"  % " OR ".join("hash=%s" for hash in hashs[i:i+50])
            cur.execute(query, tuple(hashs[i:i+50]))
            ret = [r[0] for r in cur]
            khashs = khashs.union(ret)
            i+=50
        new_hash = set(hashs).difference(khashs)
        count = len(new_hash)
        if count > 0:
            done=0
            new_hashq = queue.Queue()
            [new_hashq.put(h) for h in new_hash]
            pbar = progressbar.ProgressBar(widgets=widget("inserting torrents in db"), maxval=count).start()

            try:
                threads = []
                for i in range(0, 20):
                    t = Thread(target=insert, args=(new_hashq, pbar))
                    t.daemon = True
                    t.start()
                    threads.append(t)
                for t in threads:
                    t.join()
            finally:
                pbar.finish()
    if dir_only:
        return hashs
    else:
        query = "SELECT hash FROM torrents WHERE name IS NULL AND (dht_last_get >= DATE_SUB(NOW(), INTERVAL 1 HOUR) OR dht_last_announce >= DATE_SUB(NOW(), INTERVAL 1 HOUR) OR %s)"  % " OR ".join("hash=%s" for hash in hashs)
    cur.execute(query, tuple(hashs))
    ret = [r[0] for r in cur]
    cur.close()
    return ret

def get_dir(db=None):
    if db is None:
        db = MySQLdb.connect(**config.mysql)
    files = os.listdir("torrents/")
    hashs = [h[:-8] for h in files if h.endswith(".torrent")]
    cur = db.cursor()
    cur.execute("SELECT hash FROM torrents WHERE name IS NULL AND (%s)"  % " OR ".join("hash=%s" for hash in hashs), tuple(hashs))
    ret = [r[0] for r in cur]
    new_hash = set(hashs).difference(ret)
    count = len(new_hash)
    done=0
    new_hashq = queue.Queue()
    [new_hashq.put(h) for h in new_hash]
    pbar = progressbar.ProgressBar(widgets=widget("inserting torrents in db"), maxval=count).start()
    cur.close()
    try:
        threads = []
        for i in range(0, 20):
            t = Thread(target=insert, args=(new_hashq, pbar))
            t.daemon = True
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
    finally:
        print("")
        db.commit()

    cur.close()
    return ret + list(new_hash)

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


def is_torcache(hash):
    resp = requests.head("http://torcache.net/torrent/%s.torrent" % hash.upper())
    return resp.status_code == 200

def fetch_torrent(db):
    cur = db.cursor()
    cur.execute("SELECT hash FROM torrents WHERE name IS NULL AND (torcache_notfound=%s OR torcache_notfound IS NULL)", (False,))
    hashs = queue.Queue()
    [hashs.put(r[0].lower()) for r in cur]
    #[hashs.put(r[0].lower()) for r in get_dir(db)]
    notfound = set()
    notfound_nb = 0
    found_nb = 0
    already_nb = 0
    cur = db.cursor()
    count = hashs.qsize()
    pbar = progressbar.ProgressBar(widgets=widget("torrents fetched"), maxval=count).start()
    c_count = 0
    def load_url(db, cur, hash):
        nonlocal c_count, notfound, notfound_nb, found_nb, already_nb
        if os.path.isfile("torrents/%s.torrent" % hash):
            update_db(db, hashs=[hash], quiet=True)
            already_nb+=1
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
                            update_db(db, hashs=[hash], torcache=True, quiet=True)
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
                hash = qhashs.get(timeout=0)
                load_url(db, cur, hash)
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
    print("%s found, %s not found, %s already gotten" % (found_nb, notfound_nb, already_nb))
    return notfound
            
def get_torrent(db, hash, base_path="torrents"):
    global downloading, failed
    if hash in failed:
        return None
    if os.path.isfile("%s/%s.torrent" % (base_path, hash)):
        torrent = open("%s/%s.torrent" % (base_path, hash), 'rb').read()
        try:
            torrent = utils.bdecode(torrent)
            if not b'info' in torrent:
                return {b'info':torrent}
            else:
                return torrent
        except utils.BcodeError as e:
            print("FAILED %s: %r" % (hash, e))
            failed.add(hash)
    else:
        return


    
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
        #print("\rWrong torrent hash %s --> %s" % (hash, real_hash))
        try:
            cur.execute("UPDATE torrents SET hash=%s WHERE hash=%s", (real_hash, hash))
        except MySQLdb.errors.IntegrityError:
            cur.execute("DELETE FROM torrents WHERE hash=%s", (hash,))
        os.rename("torrents/%s.torrent" % hash, "torrents/%s.torrent" % real_hash)
        #update_db_torrent(db, cur, real_hash, torrent, torcache=torcache)


def update_db(db=None, hashs=None, torcache=None, quiet=False):
    global failed
    if db is None:
        db = MySQLdb.connect(**config.mysql)
    cur = db.cursor()
    if hashs is None:
        quiet=True
        hashs = get_hash(db)
    if not hashs:
        return
    if quiet and len(hashs)>1:
        progress = progressbar.ProgressBar(widgets=widget("torrents"))
        hashs = progress(hashs)
        print("Update db")
    try:
        for hash in hashs:
            if os.path.isfile("torrents/%s.torrent" % hash) and not hash in failed:
                t = get_torrent(db, hash)
                if t is None:
                    continue
                update_db_torrent(db, cur, hash, t, id=None, torcache=torcache, quiet=quiet)
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
    #add_to_aria(db)
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
            last_loop = time.time()
            print("\n\n\nNEW LOOP")
            get_new_torrent()
            db = MySQLdb.connect(**config.mysql)
            update_torrent_file(db)
            notfound = fetch_torrent(db)
            #if notfound:
            #    ret = add_to_aria(db, notfound)
            #update_aria(db)
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
            db.commit()
            db.close()
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
            db = MySQLdb.connect(**config.mysql)
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
            #init_aria()
            db = MySQLdb.connect(**config.mysql)
            update_db(db)
            #add_to_aria(db)
            db.commit()
            db.close()
            db = MySQLdb.connect(**config.mysql)
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
            if not r.headers['X-Torrage-Error-Msg'].startswith('Deleted torrent:'):
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

    count = len(hashs)
    done=0
    hashsq = queue.Queue()
    [hashsq.put(h) for h in hashs]
    pbar = progressbar.ProgressBar(widgets=widget("torrents uploaded to torcache"), maxval=count).start()
    up_nb=0
    got_nb=0
    fail_nb=0
    def upload(hashsq):
        nonlocal done,up_nb, got_nb, fail_nb
        db = MySQLdb.connect(**config.mysql)
        cur = db.cursor()
        try:
            while True:
                hash = hashsq.get(timeout=0)
                tc = is_torcache(hash)
                if not tc and os.path.isfile("torrents/%s.torrent" % hash):
                    t = get_torrent(db, hash)
                    upload_to_torcache(db, hash, quiet=True)
                    up_nb+=1
                elif tc:
                    cur.execute("UPDATE torrents SET torcache=%s WHERE hash=%s", (True, hash))
                    Replicator.send_torrent([(hash, ("http://torcache.net/torrent/%s.torrent" % hash.upper()))])
                    got_nb+=1
                else:
                    cur.execute("UPDATE torrents SET torcache=%s WHERE hash=%s", (False, hash))
                    fail_nb+=1
                done+=1
                pbar.update(done)
        except queue.Empty:
            pass
        finally:
            db.commit()
            cur.close()
            db.close()

    cur.close()
    try:
        threads = []
        for i in range(0, 20):
            t = Thread(target=upload, args=(hashsq,))
            t.daemon = True
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        #for hash in hashs:
        #    load_url(db, cur, hash)
    finally:
        pbar.finish()
    print("%s uploaded, %s already upped, %s failed" % (up_nb, got_nb, fail_nb))


if __name__ == '__main__':
    #init_aria()
    loop()
