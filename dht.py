#!/usr/bin/env python
# -*- coding: utf-8 -*-
from functools import total_ordering
import os
import time
import struct
import socket
import Queue
import select
import pickle
import MySQLdb
from threading import Thread
import collections
import signal
import heapq
import Queue

import config
from utils import *
from krcp import *


import resource

resource.setrlimit(resource.RLIMIT_RSS, (5 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024)) #limit to one kilobyte


class DHT(object):
    def __init__(self, bind_port, bind_ip="0.0.0.0", root=None, id=None, ignored_ip=[], debuglvl=0, prefix="", master=False):

        # checking the provided id or picking a random one
        if id is not None:
            check_id("", id)
            self.myid = ID(id)
        else:
            self.myid = ID()

        # initialising the routing table
        self.root = BucketTree(bucket=Bucket(), split_ids=[self.myid]) if root is None else root
        # Map beetween transaction id and messages type (to be able to match responses)
        self.transaction_type={}
        # Token send on get_peers query reception
        self.token=collections.defaultdict(list)
        # Token received on get_peers response reception
        self.mytoken={}
        # Map between torrent hash on list of peers
        self.peers=collections.defaultdict(dict)

        self.bind_port = bind_port
        self.bind_ip = bind_ip

        self.sock = None

        self.ignored_ip = ignored_ip
        self.debuglvl = debuglvl
        self.prefix = prefix

        self.threads=[]

        self.master = master
        self.stoped = True


    def stop_bg(self):
        if not self.stoped:
            Thread(target=self.stop).start()

    def stop(self):
        if self.stoped:
            self.debug(0, "Already stoped or soping in progress")
        self.stoped = True
        if self.myid in self.root.split_ids:
            self.root.split_ids.remove(self.myid)
        self.threads = [t for t in self.threads[:] if t.is_alive()]
        while self.threads:
            self.debug(0, "Waiting for %s threads to terminate" % len(self.threads))
            time.sleep(1)
            self.threads = [t for t in self.threads[:] if t.is_alive()]
        if self.sock:
            try:self.sock.close()
            except: pass
        
    def start(self):
        if not self.myid in self.root.split_ids:
            self.root.split_ids.append(self.myid)

        self._to_send = Queue.Queue()
        self.root_heigth = 0
        self.last_clean = time.time()
        self.stoped = False
        self.root.last_merge = 0
        self.socket_in = 0
        self.socket_out = 0
        self.last_socket_stats = time.time()
        self.last_msg = time.time()
        self.last_msg_rep = time.time()
        self.last_msg_list = []
        self.long_clean = time.time()
        self.init_socket()

        for f in [self._recv_loop, self._send_loop, self.routine]:
            t = Thread(target=f)
            t.start()
            self.threads.append(t)

    def is_alive(self):
        if self.threads and reduce(lambda x,y: x and y, [t.is_alive() for t in self.threads]):
            return True
        elif not self.threads and self.stoped:
            return False
        else:
            self.stop_bg()
            return True
        

    def debug(self, lvl, msg):
        if lvl <= self.debuglvl:
            print(self.prefix + msg)

    def socket_stats(self):
        now = time.time()
        in_s = self.socket_in
        self.socket_in = 0
        out_s = self.socket_out
        self.socket_out = 0
        delta = now - self.last_socket_stats
        self.last_socket_stats = now
        return (in_s, out_s, delta)

    def init_socket(self):
        self.debug(0, "init socket")
        if self.sock:
             try:self.sock.close()
             except: pass
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setblocking(0)
        self.sock.bind((self.bind_ip, self.bind_port))


    def sleep(self, t, fstop=None):
        t_int = int(t)
        t_dec = t - t_int
        for i in range(0, t_int):
            time.sleep(1)
            if self.stoped:
                if fstop:
                    fstop()
                exit(0)
        time.sleep(t_dec)


    def add_peer(self, info_hash, ip, port):
        """Stor a peer after a  announce_peer query"""
        self.peers[info_hash][(ip,port)]=time.time()

    def get_peers(self, info_hash):
        """Return peers store locallyy by remote announce_peer"""
        if not info_hash in self.peers:
            return None
        else:
           peers = [(-t,ip,port) for ((ip, port), t) in self.peers[info_hash].items()]
           # putting the more recent annonces in first
           peers.sort()
           return [struct.pack("4sH", socket.inet_aton(ip), port) for (_, ip, port) in peers[0:50]]

    def get_closest_node(self, id):
        return list(self.root.get_closest_nodes(id))
    
    def bootstarp(self):
        self.debug(0,"Bootstraping")
        find_node1=FindNodeQuery("", self.myid, self.myid)
        find_node2=FindNodeQuery("", self.myid, self.myid)
        find_node3=FindNodeQuery("", self.myid, self.myid)
        _, find_node1 = self.get_transaction_id(FindNodeResponse, find_node1)
        _, find_node2 = self.get_transaction_id(FindNodeResponse, find_node2)
        _, find_node3 = self.get_transaction_id(FindNodeResponse, find_node3)
        self.sendto(str(find_node1), ("router.utorrent.com", 6881))
        self.sendto(str(find_node2), ("genua.fr", 6880))
        self.sendto(str(find_node3), ("dht.transmissionbt.com", 6881))

    def save(self):
        myid = str(self.myid).encode("hex")
        pickle.dump(self.root, open("dht_%s.status" % myid, 'w+'))

    def load(self, file=None):
        if file is None:
            myid = str(self.myid).encode("hex")
            file = "dht_%s.status" % myid
        try:
            self.root = pickle.load(open(file))
        except IOError:
            self.bootstarp()
        self.root.info_hash = []



    def _update_node(self, id, (ip, port), typ):
        try:
            node = self.root.get_node(id)
            node.ip = ip
            node.port = port
        except NotFound:
            node = Node(id=id, ip=ip, port=port)
            self.root.add(self, node)
        if typ == "q":
            node.last_query = time.time()
        elif typ == "r":
            node.last_response = time.time()
            node.failed = 0
        else:
            raise ValueError("typ should be r or q")

    def _send_loop(self):
        while True:
            if self.stoped:
                exit(0)
            try:
                (msg, addr) = self._to_send.get(timeout=1)
                while True:
                    if self.stoped:
                        exit(0)
                    try:
                        (_,sockets,_) = select.select([], [self.sock], [], 1)
                        if sockets:
                            self.sock.sendto(msg, addr)
                            self.socket_out+=1
                            break
                    except socket.error as e:
                        if e.errno not in [11]: # 11: Resource temporarily unavailable
                            self.debug(0, "send:%r" %e )
                            raise
            except Queue.Empty:
                pass

    def sendto(self, msg, addr):
        self._to_send.put((msg, addr))

    def _recv_loop(self):
        while True:
            if self.stoped:
                exit(0)
            try:
                (sockets,_,_) = select.select([self.sock], [], [], 1)
            except socket.error as e:
                self.debug(0, "recv:%r" %e )
                raise

            if sockets:
                try:
                    data, addr = self.sock.recvfrom(4048)
                    if addr[0] in self.ignored_ip:
                        continue
                    if addr[1] < 1 or addr[1] > 65535:
                        self.debug(0, "Port should be whithin 1 and 65535, not %s" % addr[1])
                        continue
                    # Building python object from bencoded data
                    obj, obj_opt = self._decode(data, addr)
                    # On query
                    if obj.y == "q":
                        # Update sender node in routing table
                        self._update_node(obj["id"], addr, "q")
                        # process the query
                        self._process_query(obj)
                        # build the response object
                        reponse = obj.response(self)

                        self.socket_in+=1
                        self.last_msg = time.time()
                        self.last_msg_list.append(obj)

                        # send it
                        self.sendto(str(reponse), addr)
                    # on response
                    elif obj.y == "r":
                        # Update sender node in routing table
                        self._update_node(obj["id"], addr, "r")
                        # process the response
                        self._process_response(obj, obj_opt)

                        self.socket_in+=1
                        self.last_msg = time.time()
                        self.last_msg_rep = time.time()
                        self.last_msg_list.append(obj)
                    # on error
                    elif obj.y == "e":
                        # process it
                        self.on_error(obj, obj_opt)

                # if we raised a BError, send it
                except BError as error:
                    self.sendto(str(error), addr)
                # if unable to bdecode, malformed packet"
                except BcodeError:
                    self.sendto(str(ProtocolError("", "malformed packet")), addr)
                # socket unavailable ?
                except socket.error as e:
                    if e.errno not in [11]: # 11: Resource temporarily unavailable
                        self.debug(0, "send:%r : (%r, %r)" % (e, data, addr))
                        raise

                
    def get_transaction_id(self, reponse_type, query, id_len=4):
        id = random(id_len)
        if id in self.transaction_type:
            return self.get_transaction_id(reponse_type, query, id_len=id_len+1)
        self.transaction_type[id] = (reponse_type, time.time(), query)
        query.t = id
        return (id, query)

    def get_token(self, ip):
        """Generate a token for `ip`"""
        if ip in self.token and self.token[ip][-1][1] < 300:
            #self.token[ip] = (self.token[ip][0], time.time())
            return self.token[ip][-1][0]
        else:
            id = random(4)
            self.token[ip].append((id, time.time()))
            return id

    def get_valid_token(self, ip):
        """Return a list of valid tokens for `ip`"""
        if ip in self.token:
            now = time.time()
            return [t[0] for t in self.token[ip] if (now - t[1]) < 600]
        else:
            return []

    def clean(self):
        pass
    def clean_long(self):
        pass

    def _clean(self):
        now = time.time()
        if now - self.last_clean < 15:
            return

        for id in self.transaction_type.keys():
            if now - self.transaction_type[id][1] > 30:
                del self.transaction_type[id]

        self.threads = [t for t in self.threads[:] if t.is_alive()]

        if now - self.last_msg > 2 * 60:
            self.debug(0, "No msg since more then 2 minutes")
            self.stop()
        elif now - self.last_msg_rep > 5 * 60:
            self.debug(0, "No msg response since more then 5 minutes")
            self.stop()

        self.clean()

        # Long cleaning
        if now - self.long_clean >= 15 * 60:
            # cleaning old tokens
            for ip in self.token.keys():
                self.token[ip] = [t for t in self.token[ip] if (now - t[1]) < 600]
                if not self.token[ip]:
                    del self.token[ip]
            for id in self.mytoken.keys():
                if now - self.mytoken[id][1] > 600:
                    del self.mytoken[id]

            # cleaning old peer for announce_peer
            for hash, peers in self.peers.items():
                for peer in peers.keys():
                    if now - self.peers[hash][peer] > 15 * 60:
                        del self.peers[hash][peer]
                if not self.peers[hash]:
                    del self.peers[hash]

                # cleaning the rooting table
                if now - self.root.last_merge > 15 * 60:
                    self.root.last_merge = now
                    t = Thread(target=self.root.merge, args=(self,))
                    t.start()
                    self.threads.append(t)

            self.clean_long()

            self.long_clean = now

        self.last_clean = now

    def routine(self):
        last_explore_tree = 0
        while True:
            if self.stoped:
                exit(0)
            self.sleep(15)
            now = time.time()
            self._clean()
            if self.root_heigth != self.root.heigth():
                nodes = self.get_closest_node(self.myid)
                if nodes:
                    self.root_heigth = self.root.heigth()
                for node in nodes:
                    node.find_node(self, self.myid)
            (nodes, goods, bads) = self.root.stats()
            if goods == 0:
                self.bootstarp()
            (in_s, out_s, delta) = self.socket_stats()
            self.debug(1 if in_s > 10 and goods > 100 else 0, "%d nodes, %d goods, %d bads | in: %s, out: %s en %ss" % (nodes, goods, bads, in_s, out_s, int(delta)))
            if in_s < 5 and self.last_msg_list:
                self.debug(0, "\n".join("%r" % o for o in self.last_msg_list))
            self.last_msg_list = []
            if time.time() - last_explore_tree < 60:
                continue
            last_explore_tree = time.time()
            for bucket in self.root:
                if self.stoped:
                    exit(0)
                if now - bucket.last_changed > 15 * 60:
                    id = bucket.random_id()
                    good = [node for node in bucket if node.good]
                else:
                    id = None
                questionable = [node for node in bucket if not node.good and not node.bad]
                questionable.sort()
                if id and good:
                    good[-1].find_node(self, id)
                elif id and questionable:
                    questionable[-1].find_node(self, id)
                elif id:
                    nodes = self.get_closest_node(id)
                    nodes.sort()
                    if nodes:
                        nodes[-1].find_node(self, id)
                if questionable:
                    questionable[-1].ping(self)



    def on_error(self, error, query=None):
        pass
    def on_ping_response(self, query, response):
        pass
    def on_find_node_response(self, query, response):
        pass
    def on_get_peers_response(self, query, response):
        pass
    def on_announce_peer_response(self, query, response):
        pass
    def on_ping_query(self, query):
        pass
    def on_find_node_query(self, query):
        pass
    def on_get_peers_query(self, query):
        pass
    def on_announce_peer_query(self, query):
        pass
    def _on_find_node_response(self, query, response):
        for node in response["nodes"]:
            self.root.add(self, node)

    def _on_get_peers_response(self, query, response):
        self.mytoken[response["id"]]=(response["token"], time.time())
        for node in response.r.get("nodes", []):
            self.root.add(self, node)

    def _on_announce_peer_query(self, query):
        if query.a.get("implied_port", 0) != 0:
            self.add_peer(info_hash=query.a["info_hash"], ip=query.addr[0], port=query.addr[1])
        else:
            self.add_peer(info_hash=query.a["info_hash"], ip=query.addr[0], port=query.a["port"])


    def _process_response(self, obj, query):
        try:
            getattr(self, '_on_%s_response' % obj.q)(query, obj)
        except AttributeError:
            pass
        try:
            getattr(self, 'on_%s_response' % obj.q)(query, obj)
        except AttributeError:
            pass
    def _process_query(self, obj):
        try:
            getattr(self, '_on_%s_query' % obj.q)(obj)
        except AttributeError:
            pass
        try:
            getattr(self, 'on_%s_query' % obj.q)(obj)
        except AttributeError:
            pass

    def _decode(self, s, addr):
        d = bdecode(s)
        if not isinstance(d, dict):
            raise ProtocolError("", "Message send is not a dict")
        if not "t" in d:
            raise ProtocolError("", "Message malformed: t key is mandatory")
        try:
            if d["y"] == "q":
                if d["q"] == "ping":
                    return PingQuery(d["t"], d["a"]["id"], addr), None
                elif d["q"] == "find_node":
                    return FindNodeQuery(d["t"], d["a"]["id"], d["a"]["target"], addr), None
                elif d["q"] == "get_peers":
                    return GetPeersQuery(d["t"], d["a"]["id"], d["a"]["info_hash"], addr), None
                elif d["q"] == "announce_peer":
                    return AnnouncePeerQuery(d["t"], d["a"]["id"], d["a"]["info_hash"], d["a"]["port"], d["a"]["token"], d["a"].get("implied_port", None), addr), None
                else:
                    raise MethodUnknownError(d["t"], "Method %s is unknown" % d["q"])
            elif d["y"] == "r":
                if d["t"] in self.transaction_type:
                    ttype = self.transaction_type[d["t"]][0]
                    query = self.transaction_type[d["t"]][2]
                    if ttype == PingResponse:
                        return PingResponse(d["t"], d["r"]["id"], addr), query
                    elif ttype == FindNodeResponse:
                        return FindNodeResponse(d["t"], d["r"]["id"], Node.from_compact_infos(d["r"].get("nodes", "")), addr), query
                    elif ttype == GetPeersResponse:
                        if "values" in d["r"]:
                            return GetPeersResponse(d["t"], d["r"]["id"], d["r"]["token"], values=d["r"]["values"], addr=addr), query
                        elif "nodes" in d["r"]:
                            return GetPeersResponse(d["t"], d["r"]["id"], d["r"]["token"], nodes=Node.from_compact_infos(d["r"]["nodes"]), addr=addr), query
                        else:
                            raise ProtocolError(d["t"], "get_peers responses should have a values key or a nodes key")
                    elif ttype == AnnouncePeerResponse:
                        return AnnouncePeerResponse(d["t"], d["r"]["id"], addr), query
                    else:
                        raise MethodUnknownError(d["t"], "Method unknown %s" % ttype.__name__)
                else:
                    raise GenericError(d["t"], "transaction id unknown")
            elif d["y"] == "e":
                self.debug(2, "ERROR:%r pour %r" % (d, self.transaction_type.get(d["t"], {})))
                query = self.transaction_type.get(d["t"], (None, None, None))[2]
                if d["e"][0] == 201:
                    return GenericError(d["t"], d["e"][1]), query
                elif d["e"][0] == 202:
                    return ServerError(d["t"], d["e"][1]), query
                elif d["e"][0] == 203:
                    return ProtocolError(d["t"], d["e"][1]), query
                elif d["e"][0] == 204:
                    return MethodUnknownError(d["t"], d["e"][1]), query
                else:
                    raise MethodUnknownError(d["t"], "Error code %s unknown" % d["e"][0])
            else:
                self.debug(0, "UNKNOWN MSG: %r" % d)
                raise ProtocolError(d["t"])
        except KeyError as e:
            raise ProtocolError(d["t"], "Message malformed: %s key is missing" % e.message)
        except IndexError:
            raise ProtocolError(d["t"], "Message malformed")



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
        super(Crawler, self).start()
        self.determine_info_hash_list = []
        if self.master:
            self.root.hash_to_ignore = self.get_hash_to_ignore()
            self.root.last_update_hash = {}
            self.root.bad_info_hash = {}
            self.root.unknown_info_hash = {}
            self.root.good_info_hash = {}
        for f in [self.determine_info_hash_loop]:
            t = Thread(target=f)
            t.start()
            self.threads.append(t)

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
            for hash in self.root.unknown_info_hash.keys():
                try:
                    if now - self.root.unknown_info_hash[hash] > 30 * 60:
                        del self.root.unknown_info_hash[hash]
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
            try: del self.root.unknown_info_hash[info_hash]
            except KeyError: pass
            self.update_hash(info_hash, get=False)

    def on_announce_peer_response(self, query, response):
        info_hash = query["info_hash"]
        self.root.good_info_hash[info_hash]=time.time()
        try: del self.root.bad_info_hash[info_hash]
        except KeyError: pass
        try: del self.root.unknown_info_hash[info_hash]
        except KeyError: pass
        self.update_hash(info_hash, get=False)

    def on_ping_query(self, query):
        pass
    def on_find_node_query(self, query):
        pass
    def on_get_peers_query(self, query):
        if query.a["info_hash"] in self.root.good_info_hash:
            self.update_hash(query.a["info_hash"], get=True)
        elif not query.a["info_hash"] in self.root.bad_info_hash and not query.a["info_hash"] in self.root.unknown_info_hash and not query.a["info_hash"] in self.root.hash_to_ignore:
            self.determine_info_hash(query.a["info_hash"])

    def on_announce_peer_query(self, query):
        info_hash = query.a["info_hash"]
        self.root.good_info_hash[info_hash]=time.time()
        try: del self.root.bad_info_hash[info_hash]
        except KeyError: pass
        try: del self.root.unknown_info_hash[info_hash]
        except KeyError: pass
        self.update_hash(query.a["info_hash"], get=False)

    def get_hash_to_ignore(self, errornb=0):
        db = MySQLdb.connect(**config.mysql)
        try:
            cur = db.cursor()
            cur.execute("SELECT hash FROM torrents WHERE name IS NOT NULL")
            hashs = set([r[0].decode("hex") for r in cur])
            self.debug(0, "Returning %s hash to ignore" % len(hashs))
            db.close()
            return hashs
        except (MySQLdb.Error, ) as e:
            try:
                db.close()
            except:
                pass
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
            self.root.last_update_hash[info_hash] = time.time()
        except (MySQLdb.Error, ) as e:
            try:
                self.db.commit()
                self.db.close()
            except:
                pass
            self.debug(0, "%r" % e)
            if errornb > 10:
                raise
            time.sleep(0.1)
            self.db = MySQLdb.connect(**config.mysql)
            self.update_hash(info_hash, get, errornb=1+errornb)

    def determine_info_hash_loop(self):
        def on_stop(hash):
            try: self.root.info_hash.remove(hash)
            except ValueError: pass
            try: del self.root.unknown_info_hash[hash]
            except KeyError: pass

        def stop():
            while self.determine_info_hash_list:
                on_stop(self.determine_info_hash_list.pop()[1])

        while True:
            tosleep = 1
            while self.determine_info_hash_list:
                if self.stoped:
                    stop()
                    exit(0)
                # fetch next hash to process
                (ts, hash, tried_nodes) = heapq.heappop(self.determine_info_hash_list)
                # if process time is in the past process it
                if ts <= time.time():
                    # get hash k closest node that have not been tried
                    closest = [node for node in self.get_closest_node(hash) if node not in tried_nodes]
                    if closest:
                        node = closest[0]
                        try:
                            # send a get peer to the closest
                            node.get_peers(self, hash)
                        except socket.error as e:
                            self.debug(0, "%r %r" % (e, (node.ip, node.port)))
                        tried_nodes.add(node)
                        ts = time.time() + 5
                        # If the hash has been marked good (by another thread), clear info on this hash
                        if hash in self.root.good_info_hash:
                            self.debug(1, "Hash %s is good" % hash.encode("hex"))
                            on_stop(hash)
                        # Else had it the the heap to be processed later
                        else:
                            heapq.heappush(self.determine_info_hash_list, (ts, hash, tried_nodes))
                    else:
                        self.debug(1, "Hash %s is bad" % format_hash(hash))
                        self.root.bad_info_hash[hash]=time.time()
                        on_stop(hash)
                else:
                    # if fetch time in the future, sleep until that date
                    tosleep = max(1, ts - time.time())
                    heapq.heappush(self.determine_info_hash_list, (ts, hash, tried_nodes))
                    break
            self.sleep(tosleep, stop)
            
    def determine_info_hash(self, hash):
        if hash in self.root.good_info_hash or hash in self.root.bad_info_hash or hash in self.root.unknown_info_hash or hash in self.root.hash_to_ignore:
            return
        else:
            self.root.unknown_info_hash[hash]=time.time()
            self.debug(1, "Determining hash %s" % format_hash(hash))
            if not hash in self.root.info_hash:
                self.root.info_hash.append(hash)
            tried_nodes = set()
            ts = time.time() + 15
            heapq.heappush(self.determine_info_hash_list, (ts, hash, tried_nodes))
            
class BucketFull(Exception):
    pass

class NoTokenError(Exception):
    pass

@total_ordering
class Node(object):
    def __init__(self, id, ip, port, conn=None):
        if not port > 0 and port < 65536:
            raise ValueError("Invalid port number %s, sould be within 1 and 65535" % port)
        self.id = id
        self.ip = ip
        self.port = port
        self.last_response = 0
        self.last_query = 0
        self.failed = 0

    def __repr__(self):
        return "Node: %s:%s" % (self.ip, self.port)

    def compact_info(self):
        return struct.pack("!20s4sH", str(self.id), socket.inet_aton(self.ip), self.port)

    @classmethod
    def from_compact_infos(cls, infos):
        nodes = []
        length = len(infos)
        if length/26*26 != length:
            raise ProtocolError(d["t"], "nodes length should be a multiple of 26")
        i=0
        while i < length:
            try:
                nodes.append(Node.from_compact_info(infos[i:i+26]))
            except ValueError as e:
                print("%s" % e)
            i += 26
        return nodes

    @classmethod
    def from_compact_info(cls, info):
        if len(info) != 26:
            raise EnvironmentError("compact node info should be 26 chars long")
        (id, ip, port) = struct.unpack("!20s4sH", info)
        ip = socket.inet_ntoa(ip)
        id = ID(id)
        return cls(id, ip, port)

    @property
    def good(self):
        now = time.time()
        # A good node is a node has responded to one of our queries within the last 15 minutes.
        # A node is also good if it has ever responded to one of our queries and has sent us a query within the last 15 minutes.
        return ((now - self.last_response) < 15 * 60) or (self.last_response > 0 and (now - self.last_query) < 15 * 60)

    @property
    def bad(self):
        # Nodes become bad when they fail to respond to multiple queries in a row.
        return not self.good and self.failed > 3

    def __lt__(self, other):
        if isinstance(other, Node):
            max(self.last_response, self.last_query) < max(other.last_response, other.last_query)
        else:
            raise TypeError("unsupported operand type(s) for <: 'Node' and '%s'" % type(other).__name__)

    def __eq__(self, other):
        if isinstance(other, Node):
            return self.id == other.id
        else:
            return False

    def __hash__(self):
        return hash(self.id)

    def ping(self, dht):
        msg = PingQuery("", dht.myid)
        t, msg = dht.get_transaction_id(PingResponse, msg)
        self.failed+=1
        dht.sendto(str(msg), (self.ip, self.port))

    def find_node(self, dht, target):
        msg = FindNodeQuery("", dht.myid, target)
        t, msg = dht.get_transaction_id(FindNodeResponse, msg)
        self.failed+=1
        dht.sendto(str(msg), (self.ip, self.port))

    def get_peers(self, dht, info_hash):
        msg = GetPeersQuery("", dht.myid, info_hash, )
        t, msg = dht.get_transaction_id(GetPeersResponse, msg)
        self.failed+=1
        dht.sendto(str(msg), (self.ip, self.port))

    def announce_peer(self, dht, info_hash, port):
        if self.id in dht.mytoken and (time.time() - dht.mytoken[self.id][1]) < 600:
            token = dht.mytoken[self.id][0]
            msg = AnnouncePeerQuery("", dht.myid, info_hash, port, token)
            t, msg = dht.get_transaction_id(AnnouncePeerResponse, msg)
            self.failed+=1
            dht.sendto(str(msg), (self.ip, self.port))

        else:
            raise NoTokenError()

class Bucket(list):
    max_size = 8
    last_changed = 0

    def own(self, id):
        if id.startswith(self.id[:self.id_length/8]):
            for i in range(self.id_length/8*8, self.id_length):
                if nbit(self.id, i) !=  nbit(id, i):
                    return False
            return True
        else:
            return False

    def __init__(self, id="", id_length=0):
        self.id = id
        self.id_length = id_length # en bit

    def random_id(self):
        id = ID()
        id_length = self.id_length
        id_end = id[id_length/8]
        tmp = ''
        if id_length>0:
            try:
               id_start = self.id[id_length/8]
            except IndexError:
                id_start = "\0"
            for i in range((id_length % 8)):
                tmp +=str(nbit(id_start, i))
        for i in range((id_length % 8), 8):
            tmp +=str(nbit(id_end, i))
        try:
            char = chr(int(tmp, 2))
        except ValueError:
            print tmp
            raise
        return ID(self.id[0:id_length/8] + char + id[id_length/8+1:])

    def get_node(self, id):
        for n in self:
            if n.id == id:
                return n
        raise NotFound()

    def add(self, dht, node):
        if not self.own(node.id):
            raise ValueError("Wrong Bucket")
        elif node in self:
            try:
                old_node = self.get_node(node.id)
                old_node.ip = node.ip
                old_node.port = node.port
                self.last_changed = time.time()
            except NotFound:
                try:
                    self.remove(node)
                except: pass
        elif len(self) < self.max_size:
            self.append(node)
            self.last_changed = time.time()
        else:
            for n in self:
                if n.bad:
                    try:
                        self.remove(n)
                    except ValueError:
                        pass
                    self.add(dht, node)
                    return
            l=list(self)
            l.sort()
            if not l[-1].good:
                l[-1].ping(dht)
            raise BucketFull()

    def split(self, dht):
        if self.id_length < 8*len(self.id):
            new_id = self.id
        else:
            new_id = self.id + "\0"
        b1 = Bucket(id=new_id, id_length=self.id_length + 1)
        b2 = Bucket(id=nflip(new_id, self.id_length), id_length=self.id_length + 1)
        for node in self:
            if b1.own(node.id):
                b1.add(dht, node)
            else:
                b2.add(dht, node)
        if nbit(b1.id, self.id_length) == 0:
            return (b1, b2)
        else:
            return (b2, b1)


    @property
    def to_refresh(self):
        return time.time() - self.last_changed > 15 * 60

class NotFound(Exception):
    pass

class BucketTree(object):

    def __init__(self, bucket=None, zero=None, one=None, parent=None, level=0, split_ids=[], info_hash=[]):
        self.zero = zero
        self.one = one
        self.bucket = bucket
        self.level = level
        self.parent = parent
        self.split_ids=split_ids
        self.info_hash = info_hash
        self.last_merge = 0
        #self.good_info_hash = {}
        #self.bad_info_hash = {}
        #self.unknown_info_hash = {}
        #self.hash_to_ignore=set()
        #self.last_update_hash = {}

    def own(self, id):
        if self.bucket is not None:
            return self.bucket.own(id)
        else:
            return self.one.own(id) or self.zero.own(id)

    def stats(self):
        nodes = 0
        goods = 0
        bads = 0
        others = 0
        try:
            for b in self:
                for n in b:
                    nodes+=1
                    if n.good:
                        goods+=1
                    elif n.bad:
                        bads+=1
                    else:
                        others+=1
        except (TypeError, AttributeError):
            pass 
        return (nodes, goods, bads)

    def heigth(self):
        if self.bucket is None:
            try:
                h = max(self.zero.heigth(), self.one.heigth())
            except AttributeError:
                h = 1
            return 1 + h
        else:
            return 0

    def __str__(self):
        ret = ""
        if self.bucket is None:
            ret += str(self.zero)
            ret += str(self.one)
        else:
            ret += str(self.bucket) + "\n"
        return ret

    def __iter__(self):
        stack = [self]
        while stack:
            try:
                b = stack.pop()
                bucket = b.bucket
                if bucket is None:
                    zero = b.zero
                    if zero is not None:
                        stack.append(zero)
                    one = b.one
                    if one is not None:
                        stack.append(one)
                else:
                    yield bucket
            except AttributeError as e:
                print("%r" % e)

    def _find(self, id):
        try:
            bit = nbit(id, self.level)
        except IndexError:
            bit = 0
        if bit == 0 and self.zero:
            return self.zero._find(id)
        elif bit == 1 and self.one:
            return self.one._find(id)
        elif self.bucket is not None:
            return self
        else:
            raise EnvironmentError("Empty leave")

    def get_node(self, id):
        b = self.find(id)
        return b.get_node(id)

    def find(self, id):
        bucket = self._find(id).bucket
        i = 0
        while bucket is None and i < 100:
            bucket = self._find(id).bucket
            i+=1
        if i >= 100:
            raise EnvironmentError("Unable to find non None bucket")
        return bucket

    def get_nodes(self):
        bucket = self.bucket
        one = self.one
        zero = self.zero
        if bucket is not None:
            return [n for n in bucket]
        else:
            if one is not None:
                nodes1 = one.get_nodes()
            else:
                nodes1 = []
            if zero is not None:
                nodes0 = zero.get_nodes()
            else:
                nodes0 = []
            return nodes1 + nodes0

    def get_closest_nodes(self, id, bt=None, nodes=None, done=None):
        if not isinstance(id, ID):
            id = ID(id)
        bt = self._find(id)
        nodes = bt.get_nodes()
        while len(nodes) < Bucket.max_size and bt.parent:
            bt = bt.parent
            nodes = bt.get_nodes()
        nodes.sort(key=lambda x:id ^ x.id)
        return nodes[0:Bucket.max_size]

    def add(self, dht, node):
        b = self.find(node.id)
        try:
            b.add(dht, node)
        except BucketFull:
            for id in self.split_ids + self.info_hash:
                if b.own(id):
                    self.split(dht, node.id)
                    self.add(dht, node)
                    return

    def split(self, dht, id):
        bt = self._find(id)
        (zero_b, one_b) = bt.bucket.split(dht)
        bt.zero = BucketTree(bucket=zero_b, parent=bt, level=bt.level+1, split_ids=self.split_ids, info_hash=self.info_hash)
        bt.one = BucketTree(bucket=one_b, parent=bt, level=bt.level+1, split_ids=self.split_ids, info_hash=self.info_hash)
        bt.bucket = None

    def merge(self, dht):
        if dht.stoped:
            exit(0)
        elif self.bucket is None:
            self.one.merge(dht)
            if self.zero is not None:
                self.zero.merge(dht)
        elif self.parent and self.parent.one.bucket is None:
            self.parent.one.merge(dht)
        elif self.parent and self.parent.zero.bucket is None:
            self.parent.zero.merge(dht)
        else:
            to_merge =  True
            for id in self.split_ids + self.info_hash:
                if not self.parent or self.parent.bucket is not None or self.parent.zero.own(id) or self.parent.one.own(id):
                    to_merge = False
                    break
            if to_merge:
                bucket0 = self.parent.zero.bucket
                bucket1 = self.parent.one.bucket
                bt = self.parent
                self.parent = None
                bt.bucket = bucket0
                bt.bucket.id_length -= 1
                bt.zero = None
                bt.one = None
                for node in bucket1:
                    bt.add(dht, node)
                
class RoutingTable(object):
    root = BucketTree(bucket=Bucket())
    def __init__(self, (boostrap_ip, boostrap_port)):
        pass


debug = 0

id_base = ID('\x8c\xc4[\xb1\xae\x8c\x8b\x00\x98dz\xd7%\xc3\x12\xda\xc4iSl')
#ignored_ip = ["188.165.207.160", "10.8.0.1", "10.9.0.1", "192.168.10.1", "192.168.10.100", "192.168.10.101"]
ignored_ip = []
port_base = 12345
prefix=1
dht_base = Crawler(bind_port=port_base, id=id_base, ignored_ip=ignored_ip, debuglvl=debug, prefix="%s:" % prefix, master=True)
#dht_base.load()
dhts = [dht_base]
for id in enumerate_ids(4, id_base):
    if id == id_base:
        continue
    prefix+=1
    dhts.append(Crawler(bind_port=port_base + ord(id[0]), id=ID(id), root=dht_base.root, ignored_ip=ignored_ip, debuglvl=debug, prefix="%s:" % prefix))

stoped = False
def lauch():
    global stoped
    try:
        for dht in dhts:
            if stoped:
                raise Exception("Stoped")
            dht.start()
            time.sleep(1.4142135623730951 * 0.3)
        while True:
            for dht in dhts:
                if stoped:
                    raise Exception("Stoped")
                if not dht.is_alive():
                    print("thread stoped, restarting")
                    dht.start()
                    #raise Exception("Stoped")
            time.sleep(10)
    except (KeyboardInterrupt, Exception) as e:
        print("%r" % e)
        stop()
        print("exit")
        #os._exit(0)
        #raise

def stop():
    global stoped
    stoped = True
    print("start stopping")
    s = []
    for dht in dhts:
        s.append(Thread(target=dht.stop))
    for t in s:
        t.start()
    s = [t for t in s if t.is_alive()]
    while s:
        time.sleep(1)
        s = [t for t in s if t.is_alive()]

def sighandler(signum, frame):
    print 'Signal handler called with signal', signum
    stop()

if __name__ == '__main__':
    #for i in [x for x in dir(signal) if x.startswith("SIG")]:
    #    try:
    #        signum = getattr(signal,i)
    #        signal.signal(signum,sighandler)
    #    except (RuntimeError, ValueError) as m:
    #        print "Skipping %s"%i

    lauch()

