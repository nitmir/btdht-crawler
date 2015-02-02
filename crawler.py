#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import time
import psutil
import socket
import struct
import MySQLdb
import collections
import multiprocessing
from threading import Thread, Lock
from btdht import DHT, ID, RoutingTable
from btdht.utils import enumerate_ids

import config
import resource
import torrent

class HashToIgnore(object):
    hash_to_ignore = set()
    hash_not_to_ignore = collections.defaultdict(int)
    db = None


    def __init__(self):
        self.init_db()
        self.lock = Lock()

    def init_db(self):
        try: self.db.close()
        except: pass
        self.db = MySQLdb.connect(**config.mysql)

    def add(self, item):
        self.hash_to_ignore.add(item)
        try:
            del self.hash_not_to_ignore[item]
        except KeyError:
            pass

    def __contains__(self, item, errno=0):
        if not item in self.hash_to_ignore:
            if time.time() - self.hash_not_to_ignore[item] > 300:
                try:
                    ret = False
                    with self.lock:
                        if self.db.cursor().execute("select (1) from torrents where hash=HEX(%s) AND created_at IS NOT NULL limit 1", (item,)):
                            ret = True
                    if ret:
                        self.hash_to_ignore.add(item)
                        try:
                            del self.hash_not_to_ignore[item]
                        except KeyError:
                            pass
                        return True
                    else:
                        self.hash_not_to_ignore[item] = time.time()
                        return False
                except (MySQLdb.Error, ) as e:
                    if errno > 5:
                        print("%r" % e)
                        raise
                    else:
                        self.init_db()
                        return self.__contains__(item, errno=errno+1)
            else:
                return False
        else:
            return True

class Crawler(DHT):
    def __init__(self, *args, **kwargs):
        super(Crawler, self).__init__(*args, **kwargs)
        if self.master:
            self.root.client = torrent.Client(debug=self.debuglvl>0)
        self.db = None
        self.register_message("get_peers")
        self.register_message("announce_peer")

    def stop(self):
        self.update_hash(None, None)
        if self.master:
            self.root.client.stoped = True
        super(Crawler, self).stop()
        if self.db:
            try:self.db.close()
            except: pass


    def start(self):
        # doing some initialisation
        if self.master:
            self.root.hash_to_ignore = HashToIgnore()
            self.root.update_hash = set()
            self.root.update_hash_lock = Lock()
            self.root.bad_info_hash = {}
            self.root.good_info_hash = {}
            self.root.last_update_hash = 0
        self.hash_to_fetch = collections.OrderedDict()
        self.hash_to_fetch_tried = collections.defaultdict(set)
        self.hash_to_fetch_totry = collections.defaultdict(set)

        # calling parent method
        super(Crawler, self).start()

        # starting threads
        for f, name in [(self._client_loop, 'client_loop')]:
            t = Thread(target=f)
            t.setName("%s:%s" % (self.prefix, name))
            t.daemon = True
            t.start()
            self._threads.append(t)
            self.threads.append(t)
        if self.master:
            # addings threads to parent threads list
            self.root.client.start()
            self._threads.extend(self.root.client.threads)
            self.threads.extend(self.root.client.threads)

    def _client_loop(self):
        failed_count = collections.defaultdict(int)
        last_fail = collections.defaultdict(int)
        while True:
            processed = False
            for hash in self.hash_to_fetch.keys():
                if self.stoped:
                    return
                if hash in self.root.client.meta_data or os.path.isfile("%s/%s.torrent" % (config.torrents_dir, hash.encode("hex"))):
                    if hash in self.root.client.meta_data and self.root.client.meta_data[hash] is not True:
                        with open("%s/%s.torrent.new" % (config.torrents_dir, hash.encode("hex")), 'wb') as f:
                            f.write("d4:info%se" % self.root.client.meta_data[hash])
                        os.rename("%s/%s.torrent.new" % (config.torrents_dir, hash.encode("hex")), "%s/%s.torrent" % (config.torrents_dir, hash.encode("hex"))) 
                        self.debug(1, "%s downloaded" % hash.encode("hex"))
                    self.root.client.meta_data[hash] = True
                    self.root.client.clean_hash(hash)
                    self.root.hash_to_ignore.add(hash)
                    try: del self.hash_to_fetch[hash]
                    except: pass
                    try: del self.hash_to_fetch_tried[hash]
                    except: pass
                    try: del self.hash_to_fetch_totry[hash]
                    except: pass
                    del self.root.client.meta_data[hash]
                else:
                    self.get_peers(hash, block=False, limit=1000)
                    if time.time() - last_fail[hash] > 10:
                        try:
                            (ip, port) = self.hash_to_fetch_totry[hash].pop()
                            self.hash_to_fetch_tried[hash].add((ip, port))
                            self.root.client.add(ip, port, hash)
                            processed = True
                        except KeyError:
                            last_fail[hash]=time.time()
                            failed_count[hash]+=1
                        if failed_count[hash] >= 18:
                            self.root.client.meta_data[hash] = True
                            self.root.client.clean_hash(hash)
                            self.root.good_info_hash[hash]=time.time()
                            try: del self.hash_to_fetch[hash]
                            except: pass
                            try: del self.hash_to_fetch_tried[hash]
                            except: pass
                            try: del self.hash_to_fetch_totry[hash]
                            except: pass
                            self.debug(1, "%s failed" % hash.encode("hex"))
                            del failed_count[hash]
                            del self.root.client.meta_data[hash]
            if not processed:
                self.sleep(10)
                
    def clean(self):
        if self.master:
            now = time.time()
            if now - self.root.last_update_hash > 60:
                self.update_hash(None, None)
                self.root.last_update_hash = now

    def clean_long(self):
        if self.master:
            now = time.time()
            # cleanng old bad info_hash
            for hash in self.root.bad_info_hash.keys():
                try:
                    if now - self.root.bad_info_hash[hash] > 30 * 60:
                        del self.root.bad_info_hash[hash]
                except KeyError:
                    pass

            for hash in self.root.good_info_hash.keys():
                try:
                    if now - self.root.good_info_hash[hash] > 30 * 60:
                        del self.root.good_info_hash[hash]
                except KeyError:
                    pass

            # Actualising hash to ignore
            #self.root.hash_to_ignore = self.get_hash_to_ignore()
            self.save()


    def on_get_peers_response(self, query, response):
        if response.get("values"):
            info_hash = query.get("info_hash")
            if info_hash:
                if not info_hash in self.hash_to_fetch and not info_hash in self.root.hash_to_ignore:
                    self.hash_to_fetch[info_hash]=time.time()
                    #self.root.good_info_hash[info_hash]=time.time()
                    try: del self.root.bad_info_hash[info_hash]
                    except KeyError: pass
                self.update_hash(info_hash, get=False)
                if info_hash in self.hash_to_fetch:
                    for ipport in response.get("values", []):
                        (ip, port) = struct.unpack("!4sH", ipport)
                        ip = socket.inet_ntoa(ip)
                        if not (ip, port) in self.hash_to_fetch_tried[info_hash]:
                            self.hash_to_fetch_totry[info_hash].add((ip, port))

    def on_get_peers_query(self, query):
        info_hash = query.get("info_hash")
        if info_hash:
            if info_hash in self.root.good_info_hash and not info_hash in self.root.hash_to_ignore:
                self.update_hash(info_hash, get=True)
            elif not info_hash in self.root.bad_info_hash and not info_hash in self.root.good_info_hash and not info_hash in self.root.hash_to_ignore and not info_hash in self.hash_to_fetch:
                self.determine_info_hash(info_hash)

    def on_announce_peer_query(self, query):
        info_hash = query.get("info_hash")
        if info_hash:
            if info_hash not in self.root.hash_to_ignore:
                self.hash_to_fetch[info_hash]=time.time()
            self.root.good_info_hash[info_hash]=time.time()
            try: del self.root.bad_info_hash[info_hash]
            except KeyError: pass
            self.update_hash(info_hash, get=False)

    def get_hash_to_ignore(self, errornb=0):
        db = MySQLdb.connect(**config.mysql)
        try:
            cur = db.cursor()
            cur.execute("SELECT UNHEX(hash) FROM torrents WHERE created_at IS NOT NULL")
            hashs = set(r[0] for r in cur)
            self.debug(0, "Returning %s hash to ignore" % len(hashs))
            cur.close()
            db.close()
            return hashs
        except (MySQLdb.Error, ) as e:
            try:cur.close()
            except:pass
            try:db.close()
            except:pass
            self.debug(0, "%r" % e)
            if errornb > 10:
                raise
            time.sleep(0.1)
            return self.get_hash_to_ignore(errornb=1+errornb)
        

    def update_hash(self, info_hash, get, errornb=0):
        if info_hash in self.root.hash_to_ignore:
            return
        with self.root.update_hash_lock:
            # Try update a hash at most once every 5 minutes
            if info_hash is not None:
                self.root.update_hash.add((info_hash, get))
                return
            else:
                hashs_get = [h for h,g in self.root.update_hash if g]
                hashs_announce = [h for h,g in self.root.update_hash if not g]
                self.root.update_hash = set()
        query_get = "INSERT INTO torrents (hash, visible_status, dht_last_get) VALUES %s ON DUPLICATE KEY UPDATE dht_last_get=NOW();" % ", ".join("(LOWER(HEX(%s)),2,NOW())" for h in hashs_get)
        query_announce = "INSERT INTO torrents (hash, visible_status, dht_last_announce) VALUES %s ON DUPLICATE KEY UPDATE dht_last_announce=NOW();" % ", ".join("(LOWER(HEX(%s)),2,NOW())" for h in hashs_announce)
        db = MySQLdb.connect(**config.mysql)
        try:
            cur = db.cursor()
            if hashs_get:
                cur.execute(query_get, hashs_get)
            if hashs_announce:
                cur.execute(query_announce, hashs_announce)
            db.commit()
            cur.close()
            db.close()
        except (MySQLdb.Error, ) as e:
            try:cur.close()
            except:pass
            try:db.commit()
            except:pass
            try:db.close()
            except:pass
            self.debug(0, "MYSQLERROR: %r, %s" % (e, errornb))

    def determine_info_hash(self, hash):
        def callback(peers):
            if peers:
                self.root.good_info_hash[hash]=time.time()
            else:
                self.root.bad_info_hash[hash]=time.time()
        self.get_peers(hash, delay=15, block=False, callback=callback, limit=1000)




def get_id(id_file):
    try:
        with open(id_file) as f:
            return ID(f.read(20))
    except IOError:
        print("generating new id")
        id = ID()
        with open(id_file, "w+") as f:
            f.write(str(id))
        return id

def lauch(debug, id_file="crawler1.id", lprefix=""):
    global stoped
    print "lauch %s" % id_file
    #resource.setrlimit(resource.RLIMIT_AS, (config.crawler_max_memory, -1)) #limit to one kilobyt
    resource.setrlimit(resource.RLIMIT_NOFILE, (4096, 4096))
    id_base = get_id(id_file)

    pidfile = "%s.pid" % id_file
    try:
        with open(pidfile) as f:
            pid = int(f.read().strip())
        psutil.Process(pid)
        print("pid %s is alive" % pid)
        return
    except (psutil.NoSuchProcess, IOError):
        pass
    pid = os.getpid()
    with open(pidfile, 'w') as f:
        f.write(str(pid))

    port_base = config.crawler_base_port
    prefix=1
    routing_table = RoutingTable(debuglvl=debug)
    dht_base = Crawler(bind_port=port_base + ord(id_base[0]), id=id_base, debuglvl=debug, prefix="%s%02d:" % (lprefix, prefix), master=True, routing_table=routing_table)
    liveness = [routing_table, dht_base]
    for id in enumerate_ids(config.crawler_instance, id_base):
        if id == id_base:
            continue
        prefix+=1
        liveness.append(Crawler(bind_port=port_base + ord(id[0]), id=ID(id), routing_table=routing_table, debuglvl=debug, prefix="%s%02d:" % (lprefix, prefix)))

    stoped = False
    try:
        for liv in liveness:
            if stoped:
                raise Exception("Stoped")
            liv.start()
            time.sleep(1.4142135623730951 * 0.3)
        dht_base.load()
        while True:
            for liv in liveness:
                if stoped:
                    raise Exception("Stoped")
                if not liv.is_alive():
                    if liv.zombie:
                        raise Exception("Stoped Zombie")
                    raise Exception("Stoped")
                    print("thread stoped, restarting")
                    liv.start()
            time.sleep(10)
    except (KeyboardInterrupt, Exception) as e:
        print("%r" % e)
        stop(liveness)
        dht_base.save()
        print("exit")

def stop(liveness):
    global stoped
    stoped = True
    print("start stopping")
    s = []
    for liv in liveness:
        s.append(Thread(target=liv.stop))
    for t in s:
        t.daemon = True
        t.start()
    s = [t for t in s if t.is_alive()]
    while s:
        time.sleep(1)
        s = [t for t in s if t.is_alive()]


def worker(debug):
    jobs = {}
    try:
        for i in range(1, config.crawler_worker + 1):
            jobs[i]=multiprocessing.Process(target=lauch, args=(debug, "crawler%s.id" % i, "W%s:" % i))
            jobs[i].start()
        stats={}
        mem={}
        while True:
            for i, p in jobs.items():

                # watch if the process has done some io
                try:
                    pp = psutil.Process(p.pid)
                    mem[i] = pp.memory_info().rss
                    c = pp.io_counters()
                    if i in stats:
                        if c[0] != stats[i][0] or c[1] != stats[i][1]:
                            stats[i] = (c[0], c[1], time.time(), 0)
                        # if no io since more than 2 min, there is a problem
                        elif time.time() - stats[i][2] > 120:
                            print("crawler%s no activity since 30s, killing" % i)
                            if stats[i][3] < 5:
                                p.terminate()
                                stats[i]=stats[i][0:3] + (stats[i][3] + 1, )
                            else:
                                os.system("kill -9 %s" % p.pid)
                    else:
                        stats[i] = (c[0], c[1], time.time(), 0)
                except(psutil.NoSuchProcess, psutil.AccessDenied):
                    try: del stats[i]
                    except KeyError: pass

                if sum(mem.values()) > config.crawler_max_memory:
                    raise EnvironmentError("Reach memory limit, exiting")
                # if a worker died then respan it
                if not p.is_alive():
                    print("crawler%s died, respawning" % i)
                    jobs[i]=multiprocessing.Process(target=lauch, args=(debug, "crawler%s.id" % i, "W%s:" % i))
                    jobs[i].start()
                    try: del stats[i]
                    except KeyError: pass

            time.sleep(10)
    except (KeyboardInterrupt) as e:
        print("%r" % e)
        jobs = [j for j in jobs.values() if j.terminate() or j.is_alive()]
        for i in range(40):
            jobs = [j for j in jobs if j.is_alive()]
            if not jobs:
                break
        if jobs:
            for i in range(10):
                jobs = [j for j in jobs if j.terminate() or j.is_alive()]
                if not jobs:
                    break

if __name__ == '__main__':
    debug = 0
    #if sys.argv[1:]:
    #    lauch(debug, sys.argv[1])
    #else:
    #    lauch(debug)
    if config.crawler_worker > 1:
        worker(debug)
    else:
        lauch(debug)
