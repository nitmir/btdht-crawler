#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import time
import psutil
import socket
import struct
import collections
import multiprocessing
from threading import Thread
from btdht import DHT, ID
from btdht.utils import enumerate_ids, Scheduler

import pymongo
import netaddr
from bson.binary import Binary

import config
import resource
import torrent


class SharedObject(object):
    pass


class HashToIgnore(object):
    hash_to_ignore = set()
    hash_not_to_ignore = collections.defaultdict(int)

    def __init__(self, db=None):
        if db is None:
            self.db = pymongo.MongoClient()[config.mongo["db"]]["torrents"]
        else:
            self.db = db

    def add(self, item):
        self.hash_to_ignore.add(item)
        try:
            del self.hash_not_to_ignore[item]
        except KeyError:
            pass

    def __contains__(self, item, errno=0):
        if item not in self.hash_to_ignore:
            if time.time() - self.hash_not_to_ignore[item] > 600:
                if os.path.isfile(
                    os.path.join(config.torrents_dir, "%s.torrent" % item.encode("hex"))
                ):
                    self.hash_to_ignore.add(item)
                    try:
                        del self.hash_not_to_ignore[item]
                    except KeyError:
                        pass
                    return True
                try:
                    if self.db.find(
                        {"_id": Binary(item), "status": {"$nin": [0, None]}}
                    ).count() > 0:
                        self.hash_to_ignore.add(item)
                        try:
                            del self.hash_not_to_ignore[item]
                        except KeyError:
                            pass
                        return True
                    else:
                        self.hash_not_to_ignore[item] = time.time()
                        return False
                except pymongo.errors.AutoReconnect:
                    return self.__contains__(item, errno=errno+1)
            else:
                return False
        else:
            return True


class Crawler(DHT):
    def __init__(self, *args, **kwargs):
        self.master = kwargs.get("master", False)
        if self.master:
            del kwargs["master"]
        self.share = kwargs["share"]
        del kwargs["share"]
        super(Crawler, self).__init__(*args, **kwargs)
        if self.master:
            self.share.client = torrent.Client(debug=(self.debuglvl > 0))
        self.register_message("get_peers")
        self.register_message("announce_peer")

    def stop(self):
        if self.master:
            self.share.client.stoped = True
        super(Crawler, self).stop()

    def start(self):
        # doing some initialisation
        if self.master:
            self.share.db = pymongo.MongoClient()[config.mongo["db"]]["torrents"]
            self.share.hash_to_ignore = HashToIgnore(self.share.db)
            self.share.bad_info_hash = {}
            self.share.good_info_hash = {}
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
            self.share.client.start()
            self._threads.extend(self.share.client.threads)
            self.threads.extend(self.share.client.threads)

    def _client_loop(self):
        failed_count = collections.defaultdict(int)
        last_fail = collections.defaultdict(int)
        while True:
            processed = False
            for hash in self.hash_to_fetch.keys():
                if self.stoped:
                    return
                if (
                    hash in self.share.client.meta_data or
                    os.path.isfile(
                        os.path.join(config.torrents_dir, "%s.torrent" % hash.encode("hex"))
                    )
                ):
                    if (
                        hash in self.share.client.meta_data and
                        self.share.client.meta_data[hash] is not True
                    ):
                        with open(
                            os.path.join(
                                config.torrents_dir,
                                "%s.torrent.new" % hash.encode("hex")
                            ),
                            'wb'
                        ) as f:
                            f.write("d4:info%se" % self.share.client.meta_data[hash])
                        os.rename(
                            os.path.join(
                                config.torrents_dir,
                                "%s.torrent.new" % hash.encode("hex")
                            ),
                            os.path.join(config.torrents_dir, "%s.torrent" % hash.encode("hex"))
                        )
                        self.share.db.update(
                            {'_id': Binary(hash)},
                            {"$set": {'status': 1}},
                            upsert=True
                        )
                        self.debug(-2, "%s downloaded" % hash.encode("hex"))
                    self.share.client.meta_data[hash] = True
                    self.share.client.clean_hash(hash)
                    self.share.hash_to_ignore.add(hash)
                    try:
                        del self.hash_to_fetch[hash]
                    except:
                        pass
                    try:
                        del self.hash_to_fetch_tried[hash]
                    except:
                        pass
                    try:
                        del self.hash_to_fetch_totry[hash]
                    except:
                        pass
                    del self.share.client.meta_data[hash]
                else:
                    self.get_peers(hash, block=False, limit=1000)
                    if time.time() - last_fail[hash] > 10:
                        try:
                            (ip, port) = self.hash_to_fetch_totry[hash].pop()
                            self.hash_to_fetch_tried[hash].add((ip, port))
                            self.share.client.add(ip, port, hash)
                            processed = True
                        except KeyError:
                            last_fail[hash] = time.time()
                            failed_count[hash] += 1
                        if failed_count[hash] >= 18:
                            self.share.client.meta_data[hash] = True
                            self.share.client.clean_hash(hash)
                            self.share.good_info_hash[hash] = time.time()
                            try:
                                del self.hash_to_fetch[hash]
                            except:
                                pass
                            try:
                                del self.hash_to_fetch_tried[hash]
                            except:
                                pass
                            try:
                                del self.hash_to_fetch_totry[hash]
                            except:
                                pass
                            self.debug(-1, "%s failed" % hash.encode("hex"))
                            del failed_count[hash]
                            del self.share.client.meta_data[hash]
            if not processed:
                self.sleep(10)

    def clean(self):
        pass

    def clean_long(self):
        if self.master:
            now = time.time()
            # cleanng old bad info_hash
            for hash in self.share.bad_info_hash.keys():
                try:
                    if now - self.share.bad_info_hash[hash] > 30 * 60:
                        del self.share.bad_info_hash[hash]
                except KeyError:
                    pass

            for hash in self.share.good_info_hash.keys():
                try:
                    if now - self.share.good_info_hash[hash] > 30 * 60:
                        del self.share.good_info_hash[hash]
                except KeyError:
                    pass

    def on_get_peers_response(self, query, response):
        if response.get("values"):
            info_hash = query.get("info_hash")
            if info_hash:
                if (
                    info_hash not in self.hash_to_fetch and
                    info_hash not in self.share.hash_to_ignore
                ):
                    self.hash_to_fetch[info_hash] = time.time()
                    try:
                        del self.share.bad_info_hash[info_hash]
                    except KeyError:
                        pass
                if info_hash in self.hash_to_fetch:
                    for ipport in response.get("values", []):
                        (ip, port) = struct.unpack("!4sH", ipport)
                        ip = socket.inet_ntoa(ip)
                        try:
                            if (ip, port) not in self.hash_to_fetch_tried[info_hash]:
                                if not netaddr.IPAddress(ip).is_private():
                                    self.hash_to_fetch_totry[info_hash].add((ip, port))
                                else:
                                    self.hash_to_fetch_tried[info_hash].add((ip, port))
                        except netaddr.AddrFormatError:
                            self.hash_to_fetch_tried[info_hash].add((ip, port))

    def on_get_peers_query(self, query):
        info_hash = query.get("info_hash")
        if info_hash:
            if (
                info_hash not in self.share.bad_info_hash and
                info_hash not in self.share.good_info_hash and
                info_hash not in self.share.hash_to_ignore and
                info_hash not in self.hash_to_fetch
            ):
                self.determine_info_hash(info_hash)

    def on_announce_peer_query(self, query):
        info_hash = query.get("info_hash")
        if info_hash:
            if info_hash not in self.share.hash_to_ignore:
                self.hash_to_fetch[info_hash] = time.time()
            self.share.good_info_hash[info_hash] = time.time()
            try:
                del self.share.bad_info_hash[info_hash]
            except KeyError:
                pass

    def get_hash_to_ignore(self, errornb=0):
        try:
            results = self.share.db.find(
                {"status": {"$nin": [0, None]}},
                {"_id": True, "status": False}
            )
            hashs = set(r["status"] for r in results)
            self.debug(0, "Returning %s hash to ignore" % len(hashs))
            return hashs
        except pymongo.errors.AutoReconnect as e:
            self.debug(0, "%r" % e)
            if errornb > 10:
                raise
            time.sleep(0.1)
            return self.get_hash_to_ignore(errornb=1+errornb)

    def determine_info_hash(self, hash):
        def callback(peers):
            if peers:
                self.share.good_info_hash[hash] = time.time()
            else:
                self.share.bad_info_hash[hash] = time.time()
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


def lauch(debug, id_file="crawler1.id", lprefix="", worker_alive=None):
    global stoped
    print "%slauch %s" % (lprefix, id_file)
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
    prefix = 1
    scheduler = Scheduler()
    share = SharedObject()
    dht_base = Crawler(
        bind_port=port_base + ord(id_base[0]),
        id=id_base,
        debuglvl=debug,
        prefix="%s%02d:" % (lprefix, prefix),
        master=True,
        ignored_ip=config.ignored_ip,
        ignored_net=config.ignored_net,
        scheduler=scheduler,
        share=share
    )
    liveness = [dht_base]
    for id in enumerate_ids(config.crawler_instance, id_base):
        if id == id_base:
            continue
        prefix += 1
        liveness.append(
            Crawler(
                bind_port=port_base + ord(id[0]),
                id=ID(id),
                debuglvl=debug,
                prefix="%s%02d:" % (lprefix, prefix),
                ignored_ip=config.ignored_ip,
                ignored_net=config.ignored_net,
                scheduler=scheduler,
                share=share
            )
        )

    stoped = False
    try:
        for liv in liveness:
            if stoped:
                raise Exception("Stoped")
            liv.start()
            time.sleep(1.4142135623730951 * 0.1)
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
            if worker_alive is not None and (time.time() - worker_alive.value) > 15:
                raise Exception("Manager worker exited")
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


def worker(debug):
    jobs = {}
    try:
        worker_alive = multiprocessing.Value('i', int(time.time()))
        for i in range(1, config.crawler_worker + 1):
            jobs[i] = multiprocessing.Process(
                target=lauch,
                args=(
                    debug,
                    os.path.join(config.data_dir, "crawler%s.id" % i),
                    "W%s:" % i,
                    worker_alive
                )
            )
            jobs[i].daemon = True
            jobs[i].start()
        stats = {}
        mem = {}
        while True:
            worker_alive.value = int(time.time())
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
                            print("crawler%s no activity since 2min, killing" % i)
                            if stats[i][3] < 5:
                                p.terminate()
                                stats[i] = stats[i][0:3] + (stats[i][3] + 1, )
                            else:
                                os.system("kill -9 %s" % p.pid)
                    else:
                        stats[i] = (c[0], c[1], time.time(), 0)
                except(psutil.NoSuchProcess, psutil.AccessDenied):
                    try:
                        del stats[i]
                    except KeyError:
                        pass

                if sum(mem.values()) > config.crawler_max_memory:
                    raise EnvironmentError("Reach memory limit, exiting")
                # if a worker died then respan it
                if not p.is_alive():
                    print("crawler%s died, respawning" % i)
                    jobs[i] = multiprocessing.Process(
                        target=lauch,
                        args=(debug, os.path.join(config.data_dir, "crawler%s.id" % i), "W%s:" % i)
                    )
                    jobs[i].start()
                    try:
                        del stats[i]
                    except KeyError:
                        pass

            time.sleep(10)
    except (KeyboardInterrupt, EnvironmentError) as e:
        print("%r" % e)
        jobs = [j for j in jobs.values() if j.terminate() or j.is_alive()]
        for i in range(40):
            jobs = [j for j in jobs if j.is_alive()]
            if not jobs:
                break
            time.sleep(0.1)
        if jobs:
            for i in range(10):
                jobs = [j for j in jobs if j.terminate() or j.is_alive()]
                if not jobs:
                    break
                time.sleep(0.5)


if __name__ == '__main__':
    debug = config.debug
    for dir in [
        config.torrents_dir, config.torrents_done, config.torrents_archive,
        config.torrents_new, config.torrents_error, config.data_dir
    ]:
        if not os.path.isdir(dir):
            os.mkdir(dir)
    for file in os.listdir(config.torrents_dir):
        if file.endswith('.new'):
            os.remove(os.path.join(config.torrents_dir, file))
    worker(debug)
