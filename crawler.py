#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import time
import psutil
import MySQLdb
from threading import Thread
from dht import DHT, ID, RoutingTable
from dht.utils import enumerate_ids

import config
import resource


class Crawler(DHT):
    def __init__(self, *args, **kwargs):
        super(Crawler, self).__init__(*args, **kwargs)
        self.db = None

    def stop(self):
        super(Crawler, self).stop()
        if self.db:
            try:self.db.close()
            except: pass

    def start(self):
        if self.master:
            self.root.hash_to_ignore = self.get_hash_to_ignore()
            self.root.last_update_hash = {}
            self.root.bad_info_hash = {}
            self.root.good_info_hash = {}
        super(Crawler, self).start()


    def clean_long(self):
        if self.master:
            now = time.time()
            for hash in self.root.last_update_hash.keys():
                if now - self.root.last_update_hash[hash] > 60:
                    del self.root.last_update_hash[hash]

            # cleanng old bad info_hash
            for hash in self.root.bad_info_hash.keys():
                try:
                    if now - self.root.bad_info_hash[hash] > 30 * 60:
                        del self.root.bad_info_hash[hash]
                except KeyError:
                    pass

            for hash in self.root.good_info_hash.keys():
                try:
                    if now - self.root.good_info_hash[hash] > 3600:
                        del self.root.good_info_hash[hash]
                except KeyError:
                    pass

            # Actualising hash to ignore
            self.root.hash_to_ignore = self.get_hash_to_ignore()


    def on_error(self, error, query=None):
        pass
    def on_ping_response(self, query, response):
        pass
    def on_find_node_response(self, query, response):
        pass
    def on_get_peers_response(self, query, response):
        if response.r.get("values", None):
            info_hash = query["info_hash"]
            self.root.good_info_hash[info_hash]=time.time()
            try: del self.root.bad_info_hash[info_hash]
            except KeyError: pass
            self.update_hash(info_hash, get=False)

    def on_announce_peer_response(self, query, response):
        info_hash = query["info_hash"]
        self.root.good_info_hash[info_hash]=time.time()
        try: del self.root.bad_info_hash[info_hash]
        except KeyError: pass
        self.update_hash(info_hash, get=False)

    def on_ping_query(self, query):
        pass
    def on_find_node_query(self, query):
        pass
    def on_get_peers_query(self, query):
        if query.a["info_hash"] in self.root.good_info_hash:
            self.update_hash(query.a["info_hash"], get=True)
        elif not query.a["info_hash"] in self.root.bad_info_hash and not query.a["info_hash"] in self.root.good_info_hash and not query.a["info_hash"] in self.root.hash_to_ignore:
            self.determine_info_hash(query.a["info_hash"])

    def on_announce_peer_query(self, query):
        info_hash = query.a["info_hash"]
        self.root.good_info_hash[info_hash]=time.time()
        try: del self.root.bad_info_hash[info_hash]
        except KeyError: pass
        self.update_hash(query.a["info_hash"], get=False)

    def get_hash_to_ignore(self, errornb=0):
        db = MySQLdb.connect(**config.mysql)
        try:
            cur = db.cursor()
            cur.execute("SELECT hash FROM torrents WHERE name IS NOT NULL")
            hashs = set([r[0].decode("hex") for r in cur])
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
        # Try update a hash at most once every minute
        if info_hash in self.root.last_update_hash and (time.time() - self.root.last_update_hash[info_hash]) < 60:
            return
        if len(info_hash) != 20:
            raise ProtocolError("", "info_hash should by 20B long")
        if self.db is None:
            self.db = MySQLdb.connect(**config.mysql)
        try:
            cur = self.db.cursor()
            if get:
                cur.execute("INSERT INTO torrents (hash, visible_status, dht_last_get) VALUES (%s,2,NOW()) ON DUPLICATE KEY UPDATE dht_last_get=NOW();",(info_hash.encode("hex"),))
            else:
                cur.execute("INSERT INTO torrents (hash, visible_status, dht_last_announce) VALUES (%s,2,NOW()) ON DUPLICATE KEY UPDATE dht_last_announce=NOW();",(info_hash.encode("hex"),))
            self.db.commit()
            cur.close()
            self.root.last_update_hash[info_hash] = time.time()
        except (MySQLdb.Error, ) as e:
            try:cur.close()
            except:pass
            try:self.db.commit()
            except:pass
            try:self.db.close()
            except:pass
            self.db = None
            self.debug(0, "MYSQLERROR: %r, %s" % (e, errornb))
            if errornb > 10:
                raise
            time.sleep(0.1)
            self.update_hash(info_hash, get, errornb=1+errornb)

    def determine_info_hash(self, hash):
        def callback(peers):
            if peers:
                self.root.good_info_hash[hash]=time.time()
            else:
                self.root.bad_info_hash[hash]=time.time()
        self.get_peers(hash, delay=15, block=False, callback=callback, limit=1)




def get_id(id_file):
    try:
        return ID(open(id_file).read(20))
    except IOError:
        print("generating new id")
        id = ID()
        with open(id_file, "w+") as f:
            f.write(str(id))
        return id

def lauch(debug, id_file="crawler.id"):
    global stoped
    resource.setrlimit(resource.RLIMIT_AS, (config.crawler_max_memory, -1)) #limit to one kilobyt
    id_base = get_id(id_file)

    pidfile = "%s.pid" % id_file
    try:
        pid = int(open(pidfile).read().strip())
        psutil.Process(pid)
        print("pid %s is alive" % pid)
        return
    except (psutil.NoSuchProcess, IOError):
        pass
    pid = os.getpid()
    open(pidfile, 'w').write(str(pid))

    port_base = config.crawler_base_port
    prefix=1
    routing_table = RoutingTable(debuglvl=debug)
    dht_base = Crawler(bind_port=port_base + ord(id_base[0]), id=id_base, debuglvl=debug, prefix="%s:" % prefix, master=True, routing_table=routing_table)
    liveness = [routing_table, dht_base]
    for id in enumerate_ids(config.crawler_instance, id_base):
        if id == id_base:
            continue
        prefix+=1
        liveness.append(Crawler(bind_port=port_base + ord(id[0]), id=ID(id), routing_table=routing_table, debuglvl=debug, prefix="%s:" % prefix))

    stoped = False
    try:
        for liv in liveness:
            if stoped:
                raise Exception("Stoped")
            liv.start()
            time.sleep(1.4142135623730951 * 0.3)
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

if __name__ == '__main__':
    debug = 0
    if sys.argv[1:]:
        lauch(debug, sys.argv[1])
    else:
        lauch(debug)
