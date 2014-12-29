#!/usr/bin/env python3
#http://torcache.net/torrent/8CC4411A8F65C384FD3F53C321AF8B5890A444CB.torrent
import os
import io
import gzip
#import MySQLdb
import mysql.connector as MySQLdb
import config
import urllib
import requests
import pyaria2.pyaria2 as pyaria2
import utils
import hashlib
import time
import chardet
import pycurl

aria2 = pyaria2.PyAria2(port=15859, session=os.path.join(os.path.realpath("./"),"aria2.session"))
time.sleep(1)
options = aria2.getGlobalOption()
options["bt-save-metadata"]="true"
options["bt-metadata-only"]="true"
options["bt-stop-timeout"]="0"
options["max-concurrent-downloads"]=1000000000
options['dir']=os.path.realpath("torrents/")
aria2.changeGlobalOption(options)

def get_to_download(aria2):
    hashs=[]
    for info in aria2.tellActive() + aria2.tellWaiting(0, 1000000000):
        if 'infoHash' in info:
            hashs.append(info['infoHash'])
    return hashs

downloading = get_to_download(aria2)


def get_hash():
    db = MySQLdb.connect(**config.mysql)          
    cur = db.cursor()
    cur.execute("SELECT hash FROM torrents WHERE name IS NULL AND (dht_last_get >= DATE_SUB(NOW(), INTERVAL 1 DAY) OR dht_last_announce >= DATE_SUB(NOW(), INTERVAL 1 DAY))")
    ret = [r[0].lower() for r in cur]
    db.close()
    return ret

def clean(hashs=None):
    global aria2, downloading
    if hashs is None:
        hashs = get_hash()
    for info in aria2.tellActive():
        if 'infoHash' in info:
            if not info['infoHash'] in hashs:
                gid = info["gid"]
                aria2.remove(gid)
                downloading.remove(info['infoHash'])
                print("%s too old, removed" % info['infoHash'])

def get_torrent(hash):
    global downloading
    if os.path.isfile("torrents/%s.torrent" % hash):
        torrent = open("torrents/%s.torrent" % hash, 'rb').read()
        try:
            return utils.bdecode(torrent)
        except utils.BcodeError:
            try:
                bi = io.BytesIO(torrent)
                torrent = gzip.GzipFile(fileobj=bi, mode="rb").read()
                open("torrents/%s.torrent" % hash, 'bw+').write(torrent)
                return utils.bdecode(torrent)
            except OSError:
                return torrent
    elif hash in downloading:
        pass
    else:
        try:
            response = urllib.request.urlopen("http://torcache.net/torrent/%s.torrent" % hash.upper())
            torrentz = response.read()
            bi = io.BytesIO(torrentz)
            torrent = gzip.GzipFile(fileobj=bi, mode="rb").read()
            open("torrents/%s.torrent" % hash, 'bw+').write(torrent)
            print("Found %s on torcache" % hash)
            update_db(hashs=[hash], torcache=True)
            return torrent
        except urllib.request.HTTPError:
            print("Not Found %s, adding for download" % hash)
            aria2.addUri(["magnet:?xt=urn:btih:%s" % hash])
            downloading.append(hash)

def format_size(i):
    if i > 1024**3:
        return "%s GB" % round(i/(1024.0**3), 2)
    elif i > 1024**2:
        return "%s MB" % round(i/(1024.0**2), 2)
    elif i > 1024**1:
        return "%s KB" % round(i/(1024.0**1), 2)
    else:
        return "%s B" % i

def get_torrent_info(hash):
    torrent = get_torrent(hash)
    encodings = ["UTF-8", "euc-jp", "shift_jis"]
    if not torrent:
        return
    if b'encoding' in torrent:
       encoding = torrent[b'encoding'].decode()
    else:
        encoding = chardet.detect(torrent[b'info'][b'name'])['encoding']
        if not encoding:
            encoding = "latin9"
        #print(encoding)
        #for encoding in encodings:
        #    try:
        #        name = torrent[b'info'][b'name'].decode(encoding)
        #        break
        #    except UnicodeDecodeError as e:
        #        pass
        
    name = torrent[b'info'][b'name'].decode(encoding)
    created = torrent.get(b'creation date', int(time.time()))
    files_nb = len(torrent[b'info'][b'files']) if b'files' in torrent[b'info'] else 1
    size = sum([file[b'length'] for file in torrent[b'info'][b'files']]) if b'files' in torrent[b'info'] else torrent[b'info'][b'length']
    if b'files' in torrent[b'info']:
        description=name + "\n\n"
        description+="<table>"
        for file in torrent[b'info'][b'files']:
            try:
                description+="<tr><td>%s</td><td>%s</td></tr>" % (os.path.join(*file[b"path"]).decode(encoding), format_size(file[b'length']))
            except UnicodeDecodeError:
                encoding = chardet.detect(os.path.join(*file[b"path"]))['encoding']
                try:
                    description+="<tr><td>%s</td><td>%s</td></tr>" % (os.path.join(*file[b"path"]).decode(encoding), format_size(file[b'length']))
                except UnicodeDecodeError:
                    description+="<tr><td>????</td><td>%s</td></tr>" % format_size(file[b'length'])
        description+="</table>"
    else:
        description=""
    
    return (name, created, files_nb, size, description)


failed=set()
def update_db(hashs=None, torcache=None):
    global failed
    db = MySQLdb.connect(**config.mysql)
    cur = db.cursor()
    if hashs is None:
        hashs = get_hash()
    try:
        for hash in hashs:
            if os.path.isfile("torrents/%s.torrent" % hash) and not hash in failed:
                real_hash = hashlib.sha1(utils.bencode(get_torrent(hash)[b'info'])).hexdigest()
                if real_hash == hash:
                    try:
                        print("doing %s" % hash)
                        (name, created, files_nb, size, description) = get_torrent_info(hash)
                        cur.execute("UPDATE torrents SET name=%s, description=%s, size=%s, files_count=%s, created_at=%s, torcache=%s WHERE hash=%s", (name, description, size, files_nb, time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(created)), torcache, hash))
                        print("  done %s" % name)
                    except (KeyError, UnicodeDecodeError) as e:
                        print("%r" % e)
                        failed.add(hash)
                else:
                    print("Wrong torrent hash %s != %s" % (hash, real_hash))
                    cur.execute("UPDATE torrents SET hash=%s WHERE hash=%s", (real_hash, hash))
                    os.rename("torrents/%s.torrent" % hash, "torrents/%s.torrent" % real_hash)
    finally:
        db.commit()
        db.close()



def loop():
    last_clean = 0
    while True:
        hashs=get_hash()
        for h in hashs:
            _=get_torrent(h)
        update_db()
        feed_torcache()
        if time.time() - last_clean > 10*60:
            clean(hashs)
            last_clean = time.time()
        time.sleep(30)


def upload_to_torcache(hash):
    """c = pycurl.Curl()
    c.setopt(pycurl.HEADER, 1)
    c.setopt(pycurl.RETURNTRANSFER, True)
    c.setopt(pycurl.USERAGENT, "Mozilla/4.0 (compatible;)")
    c.setopt(pycurl.URL, 'http://torcache.net/autoupload.php')
    c.setopt(pycurl.POST, True)
    c.setopt(pycurl."""
    files = {'torrent': open("torrents/%s.torrent" % hash, 'rb')}
    r = requests.post('http://torcache.net/autoupload.php', files=files)
    db = MySQLdb.connect(**config.mysql)
    cur = db.cursor()
    try:
        if 'X-Torrage-Error-Msg' in r.headers:
            cur.execute("UPDATE torrents SET torcache=%s WHERE hash=%s", (False, hash))
            print(r.headers['X-Torrage-Error-Msg'])
        elif r.headers.get('X-Torrage-Infohash', "").lower() == hash:
            cur.execute("UPDATE torrents SET torcache=%s WHERE hash=%s", (True, hash))
            print("Uploaded %s to torcache" % hash)
        elif 'X-Torrage-Infohash' in r.headers:
            update_db([hash])
    finally:
        db.commit()
        db.close()
    return r

def feed_torcache():
    db = MySQLdb.connect(**config.mysql)
    cur = db.cursor()
    cur.execute("SELECT hash FROM torrents WHERE name IS NOT NULL AND torcache IS NULL")
    hashs = [r[0].lower() for r in cur]
    for hash in hashs:
#        if os.path.isfile("torrents/%s.torrent" % hash):
            t = get_torrent(hash)
            if not b"comment" in t or not b"torcache" in t[b"comment"]:
                upload_to_torcache(hash)
            else:
                db = MySQLdb.connect(**config.mysql)
                cur = db.cursor()
                cur.execute("UPDATE torrents SET torcache=%s WHERE hash=%s", (True, hash))
                db.commit()
                db.close()
