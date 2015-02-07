#!/usr/bin/env python

import os
import sys
import zmq
import time
import struct
import random
import json
import select
import socket
from threading import Thread

pub_port = 3333
priv_port = 3334

def test_port(i):
    if not i>0 and i<65536:
        raise ValueError("port %s not valid" % i)

#def random(size):
#    with open("/dev/urandom") as f:
#        return f.read(size)

class Replicator(object):

    rep_id = '\xe6\xdf\x8e\xcc\x0cc}{\x8b\x02(m\t\xe4\xbc\xb8\nD4\xb5'

    def __init__(self, public_ip, pub_port, priv_port=None, dht_port=None, bootstrap_port=None, on_torrent_announce=None, debug=False, dht_id=None):
        """
         * `public_ip` mst be your ip address as it is seen on the global internet (no local non routable ip here)
         * `pub_port` is a tcp port on wich you will receive message from the swarm
         * `priv_port` is an udp port for discovering new peers. it will be published the the bittorent dht.
            if None, the same port than `pub_port` is used
         * `dht_port` port on which the instance of the bittorent dht will listen. If None, a random port is chosen*
         * `bootstrap_port` port on which we listen then bootstraping to the swarm. Random if None
         * `on_torrent_announce` callback to call then receiving a torrent announce. first arg in torrent hash, second arg an url where to fetch it

        `pub_port`, `priv_port` and `bootstrap_port` must be reatchable from outside for the Replicator to work.
        `pub_port`, `priv_port` should be stable over time to permit quick restart and consistant data inside the dht
        """

        self.debug = debug
        self.publisher = {}

        from btdht import dht

        self.dht = dht.DHT(routing_table=None, bind_port=dht_port, id=dht_id)
        self.dht.root.register_torrent_longterm(self.rep_id)

        self.pub_port = pub_port
        self.priv_port = priv_port if priv_port is not None else pub_port
        self.bootstrap_port = bootstrap_port

        if on_torrent_announce:
            self.on_torrent_announce = on_torrent_announce

        self.threads = []

        self.last_announce = 0
        self.last_clean = time.time()
        self.myip = public_ip
        self.stoped = True

    def on_torrent_announce(self, hash, url):
        print("%s: %s" % (hash, url))

    def start(self):
        if not self.stoped:
            return
        if self.zombie or self.dht.zombie:
            print("Zombie thread, unable to start")
            return
        self.stoped = False
        self.dht.start()
        self.init_subscriber_sock()
        self.init_publisher_sock()
        self.init_local_sock()
        self.init_sock()
        self._ready = False
        self._failed_peers = {}
        self.threads = []
        t = Thread(target=self.loop_newclient)
        t.setName("newclient")
        t.daemon = True
        t.start()
        self.threads.append(t)
        t = Thread(target=self.loop_sub)
        t.setName("sub")
        t.daemon = True
        t.start()
        self.threads.append(t)
        t = Thread(target=self.loop_announce)
        t.setName("announce")
        t.daemon = True
        t.start()
        self.threads.append(t)
        t = Thread(target=self.loop_local)
        t.setName("local")
        t.daemon = True
        t.start()
        self.threads.append(t)
        self.threads.append(self.dht)


    def stop(self):
        if self.stoped:
            return
        self.stoped = True
        threads = list(self.threads)
        threads = [t for t in threads if t.is_alive()]
        self.dht.stop()
        for i in range(0, 30):
            if threads:
                if i > 5:
                    print("Waiting for %s threads to terminate" % len(threads))
                time.sleep(1)
                threads = [t for t in threads if t.is_alive()]
            else:
                break
        if threads:
            print("Unable to stop %s threads, giving up" % len(threads))
        for (ip, pub_port, priv_port) in self.publisher.keys():
            try:
                self.sub_sock.disconnect("tcp://%s:%s" % (ip, pub_port))
            except zmq.ZMQError:
                pass
            del self.publisher[(ip, pub_port, priv_port)]

    @property
    def zombie(self):
        return bool(self.stoped and [t for t in self.threads if t.is_alive()])

    def is_alive(self):
        if self.threads and reduce(lambda x,y: x and y, [t.is_alive() for t in self.threads]):
            return True
        elif self.stoped and reduce(lambda x,y: x and y, [not t.is_alive() for t in self.threads]):
            return False
        else:
            self.stop()
            return False

    def get_peers(self):
        known = [(ip, priv_port) for (ip, _, priv_port) in self.publisher]
        known.append((self.myip, self.priv_port))
        peers = self.dht.get_peers(self.rep_id, limit=1000)
        if peers:
            return [ipp for ipp in peers if not ipp in known and not ipp in self._failed_peers]
        else:
            return []

    def announce(self):
        self.dht.announce_peer(self.rep_id, self.priv_port)

    def loop_announce(self):
        time.sleep(1)
        started = time.time()
        # trying to bootstrap for 2 min
        while not self._ready and time.time() - started < 2 * 60:
            self.dht.build_table()
            self.bootstrap()
            for i in range(10):
                time.sleep(1)
                if self.stoped:
                    return

        if not self._ready:
            if self.debug:
                print("Unable to bootstrap, trying again")
        # trying again until no valid peer found
        while not self._ready and self.get_peers():
            self.bootstrap()
            for i in range(10):
                time.sleep(1)
                if self.stoped:
                    return

        if not self._ready:
            if self.debug:
                print("Unable to bootstrap, must be first in the swarm")

        for i in range(15):
            self.announce()
        self._ready = True
        next_announce = time.time()
        next_bootstrap = time.time()
        while True:
            if self.stoped:
                return
            if time.time() >= next_bootstrap:
                next_bootstrap = time.time() + 60 * 5
                self.bootstrap()
            if time.time() >= next_announce:
                next_announce = time.time() + 60 * 5
                self.announce()
            time.sleep(1)

    def init_subscriber_sock(self):
        context = zmq.Context()
        self.sub_sock = context.socket(zmq.SUB)
        self.sub_sock.setsockopt(zmq.SUBSCRIBE, '')
        for (ip, pub_port, _) in self.publisher:
            self.sub_sock.connect("tcp://%s:%s" % (ip, pub_port))

    def add_publisher(self, ip, pub_port, priv_port):
        if not (ip, pub_port, priv_port) in self.publisher:
            if self.debug:
                print((ip, pub_port, priv_port))
            addr = "tcp://%s:%s" % (ip, pub_port)
            self.sub_sock.connect(addr)
            self.pub_sock.send(json.dumps({"q":"add_publisher", "addr":[ip, pub_port, priv_port]}), zmq.NOBLOCK)
        self.publisher[(ip, pub_port, priv_port)] = time.time()

    def clean(self):
        now = time.time()
        if now - self.last_clean > 10:
            for (ip, pub_port, priv_port) in self.publisher.keys():
                if now - self.publisher[(ip, pub_port, priv_port)] > 60:
                    try:
                        self.sub_sock.disconnect("tcp://%s:%s" % (ip, pub_port))
                    except zmq.ZMQError:
                        pass
                    del self.publisher[(ip, pub_port, priv_port)]
            for peer in self._failed_peers.keys():
                if now - self._failed_peers[peer] > 15 * 60:
                    del self._failed_peers[peer]
            self.last_clean = now


    @staticmethod
    def send_torrent(urlhash):
        context = zmq.Context()
        sock = context.socket(zmq.REQ)
        sock.connect("inproc://replication_%s" % os.getpid())
        msg = json.dumps(urlhash)
        if not isinstance(msg, bytes):
            msg = msg.encode()
        sock.send(msg, zmq.NOBLOCK)
        
    def init_local_sock(self):
        context = zmq.Context()
        self.local_sock = context.socket(zmq.REP)
        self.local_sock.bind("inproc://replication_%s" % os.getpid())

    def init_publisher_sock(self):
        context = zmq.Context()
        self.pub_sock = context.socket(zmq.PUB)
        self.pub_sock.bind("tcp://*:%s" % self.pub_port)
    
    def init_sock(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setblocking(0)
        self.sock.bind(("0.0.0.0", self.priv_port))

    def send_swarm(self, ip, port):
        context = zmq.Context()
        sock = context.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 100)
        sock.RCVTIMEO = 1000
        sock.SNDTIMEO = 1000
        sock.connect("tcp://%s:%s" % (ip, port))
        sock.send(json.dumps({"q":"swarm_list", "swarm":self.publisher.keys()}), zmq.NOBLOCK)
        #return sock.recv() == "ok"

    def bootstrap(self):
        for ip, port in self.get_peers():
            if self.stoped:
                return
            if self.debug:
                print("Trying %s:%s" % (ip, port))
            try:
                if self.bootstrap_client(ip, port):
                    return True
                else:
                    if self.debug:
                        print("failed")
                    self._failed_peers[(ip, port)] = time.time()
            except (socket.error, zmq.ZMQError) as e:
                print("%s" % e)
        return False

    def bootstrap_client(self, ip, port):
        while True:
            try:
                if self.bootstrap_port is None:
                    zport = random.randint(10000,60000)
                else:
                    zport = self.bootstrap_port
                context = zmq.Context()
                sock = context.socket(zmq.REP)
                sock.bind("tcp://*:%s" % zport)
                break
            except zmq.ZMQError as e:
                print("%r" % e)
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        while True:
            try:
                self.sock.sendto(struct.pack("!1sHH", "b", zport, self.pub_port), (ip, port))
                break
            except socket.error as e:
                print("%r" % e)
                if e.errno not in [11, 1]: # 11: Resource temporarily unavailable
                    return
        try:
            sockets = dict(poller.poll(5000))
            if sockets and sockets[sock] == zmq.POLLIN:
                data = json.loads(sock.recv(zmq.NOBLOCK))
                sock.send("ok", zmq.NOBLOCK)
                sock.close()
                if data["q"] == "swarm_list":
                    for (ip, pub_port, priv_port) in data["swarm"]:
                        if not (ip, pub_port, priv_port) in self.publisher:
                            self.sub_sock.connect("tcp://%s:%s" % (ip, pub_port))
                            self.publisher[(ip, pub_port, priv_port)]=time.time()
                    self._ready = True
                    return True
                else:
                    print(data)
        except ValueError as e:
            sock.send(str(e), zmq.NOBLOCK)
            print("%r" % e)
        return False


    def loop_newclient(self):
        time.sleep(1)
        while True:
            if self.stoped:
                sys.exit(0)
            (sockets,_,_) = select.select([self.sock], [], [], 1)
            if sockets:
                data, addr = self.sock.recvfrom(4048)
                try:
                    if data[0] == "b":
                        _, port_bootstrap, port_pub = struct.unpack("!1sHH", data)
                        if self.debug:
                            print("b: %s,%s" % (port_bootstrap, port_pub))
                        test_port(port_bootstrap)
                        test_port(port_pub)
                        self.sock.sendto(struct.pack("!1sH", "d", self.pub_port), addr)
                        self.add_publisher(addr[0], port_pub, addr[1])
                        if self.debug:
                            print("Send swarm to %s:%s" % (addr[0], port_bootstrap))
                        self.send_swarm(addr[0], port_bootstrap)
                    elif data[0] == "d":
                        _, port_pub = struct.unpack("!1sH", data)
                        if self.debug:
                            print("d: %s" % port_pub)
                        test_port(port_pub)
                        self.add_publisher(addr[0], port_pub, addr[1])
                except (ValueError, struct.error) as e:
                    print("%r: %r" % (e, data))

    def announce_torrent(self, hash, url):
        """Announce the torrent of hash hash available a url to the swarm"""
        self.pub_sock.send(json.dumps({"q":"torrent", "hash":hash.lower(), "url":url}), zmq.NOBLOCK)

    def loop_local(self):
        time.sleep(1)
        poller = zmq.Poller()
        poller.register(self.local_sock, zmq.POLLIN)
        while True:
            if self.stoped:
                return
            sockets = dict(poller.poll(1000))
            if sockets and sockets[self.local_sock] == zmq.POLLIN:
                try:
                    data = json.loads(self.local_sock.recv())
                    for (hash, url) in data:
                        self.announce_torrent(hash, url)
                    self.local_sock.send("ok", zmq.NOBLOCK)
                except ValueError as e:
                    print("%r" % e)


    def loop_sub(self):
        time.sleep(1)
        poller = zmq.Poller()
        poller.register(self.sub_sock, zmq.POLLIN)
        while True:
            if self.stoped:
                sys.exit(0)
            sockets = dict(poller.poll(1000))
            if sockets and sockets[self.sub_sock] == zmq.POLLIN:
                try:
                    data = json.loads(self.sub_sock.recv(zmq.NOBLOCK))
                    self.process(data)
                except ValueError as e:
                    print("%r" % e)

            self.clean()

            if (time.time() - self.last_announce) > 10:
                self.pub_sock.send(json.dumps({"q":"add_publisher", "addr":[self.myip, self.pub_port, self.priv_port], "swarm_size" : len(self.publisher)}), zmq.NOBLOCK)
                self.last_announce = time.time()


    def process(self, data):
        if not "q" in data:
            pass
        if data["q"] == "add_publisher":
            self.add_publisher(*data["addr"])
            if "swarm_size" in data and len(self.publisher) < data["swarm_size"]:
                self.bootstrap_client(data["addr"][0], data["addr"][2])
        elif data["q"] == "torrent":
            if self.on_torrent_announce and data.get("hash", None) and data.get("url", None):
                self.on_torrent_announce(data["hash"], data["url"])
