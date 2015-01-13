#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
import Queue
import heapq
import struct
import socket
import select
import collections
from functools import total_ordering
from threading import Thread, Lock
from random import shuffle

import datrie

from utils import *
from krcp import *



class DHT(object):
    def __init__(self, routing_table=None, bind_port=None, bind_ip="0.0.0.0", id=None, ignored_ip=[], debuglvl=0, prefix="", master=False):

        # checking the provided id or picking a random one
        if id is not None:
            check_id("", id)
            self.myid = ID(id)
        else:
            self.myid = ID()

        # initialising the routing table
        self.root = RoutingTable() if routing_table is None else routing_table
        # Map beetween transaction id and messages type (to be able to match responses)
        self.transaction_type={}
        # Token send on get_peers query reception
        self.token=collections.defaultdict(list)
        # Token received on get_peers response reception
        self.mytoken={}
        # Map between torrent hash on list of peers
        self._peers=collections.defaultdict(collections.OrderedDict)
        self._get_peer_loop_list = []
        self._get_peer_loop_lock = {}
        self._get_closest_loop_lock = {}

        self.bind_port = bind_port
        self.bind_ip = bind_ip

        self.sock = None

        self.ignored_ip = ignored_ip
        self.debuglvl = debuglvl
        self.prefix = prefix

        self._threads=[]
        self.threads = []

        self.master = master
        self.stoped = True
        self.zombie = False
        self._threads_zombie = []


    def stop_bg(self):
        if not self.stoped:
            Thread(target=self.stop).start()

    def stop(self):
        if self.stoped:
            self.debug(0, "Already stoped or soping in progress")
            return
        self.stoped = True
        self.root.release_dht(self)
        self._threads = [t for t in self._threads[:] if t.is_alive()]
        #self.debug(0, "Trying to terminate thread for 1 minutes")
        for i in range(0, 60):
            if self._threads:
                if i > 3:
                    self.debug(0, "Waiting for %s threads to terminate" % len(self._threads))
                time.sleep(1)
                self._threads = [t for t in self._threads[:] if t.is_alive()]
            else:
                break
        if self._threads:
            self.debug(0, "Unable to stop %s threads, giving up" % len(self._threads))
            self.zombie = True
            self._threads_zombie.extend(self._threads)
            self._threads = []
        
        if self.sock:
            try:self.sock.close()
            except: pass
        
    def start(self):
        if not self.stoped:
            self.debug(0, "Already started")
            return
        if self.zombie:
            self.debug(0, "Zombie threads, unable de start")
            return self._threads_zombie
        self.root.register_dht(self)


        if self.root.stoped:
            self.root.start()
        self.root_heigth = 0
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

        for f in [self._recv_loop, self._send_loop, self._routine, self._get_peers_closest_loop]:
            t = Thread(target=f)
            t.daemon = True
            t.start()
            self._threads.append(t)
            self.threads.append(t)

    def is_alive(self):
        if self.threads and reduce(lambda x,y: x and y, [t.is_alive() for t in self.threads]):
            return True
        elif not self._threads and self.stoped:
            return False
        else:
            self.debug(0, "One thread died, stopping dht")
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
        self._to_send = Queue.Queue()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setblocking(0)
        if self.bind_port:
            self.sock.bind((self.bind_ip, self.bind_port))
        else:
            self.sock.bind((self.bind_ip, 0))
            self.bind_port = self.sock.getsockname()[1]


    def sleep(self, t, fstop=None):
        if t>0:
            t_int = int(t)
            t_dec = t - t_int
            for i in range(0, t_int):
                time.sleep(1)
                if self.stoped:
                    if fstop:
                        fstop()
                    sys.exit(0)
            time.sleep(t_dec)


    def announce_peer(self, info_hash, port, delay=0, block=True):
        """Announce info_hash on port to the K closest node from info_hash found in the dht"""
        def callback(nodes):
            for node in nodes:
                try:
                    node.announce_peer(self, info_hash, port)
                except NoTokenError:
                    node.get_peers(self, info_hash)
                    self.debug(1, "No token to announce on node %s" % node)
        if block:
            while info_hash in self._get_closest_loop_lock:
                self.sleep(1)
        if not info_hash in self._get_closest_loop_lock:
            self._get_closest_loop_lock[info_hash]=time.time()
            self.debug(2, "get closest hash %s" % info_hash.encode("hex"))
            self.root.register_torrent(info_hash)
            tried_nodes = set()
            ts = time.time() + delay
            closest = self.get_closest_nodes(info_hash)
            typ = "closest"
            heapq.heappush(self._get_peer_loop_list, (ts, info_hash, tried_nodes, closest, typ, callback, None))
            if block:
                while info_hash in self._get_closest_loop_lock:
                    self.sleep(1)

    def _add_peer(self, info_hash, ip, port):
        """Store a peer after a  announce_peer query"""
        self._peers[info_hash][(ip,port)]=time.time()
        # we only keep at most 50 peers per hash
        if len(self._peers[info_hash]) > 50:
            self._peers[info_hash].popitem(False)

    def get_peers(self, hash, delay=0, block=True, callback=None, limit=10):
        """Return a list of at most 50 (ip, port) downloading hash or pass-it to callback"""
        peers = None
        if hash in self._peers and self._peers[hash] and len(self._peers[hash])>=limit:
            peers = self._get_peers(hash, compact=False)
            if callback:
                callback(peers)
            return peers
        elif hash in self._get_peer_loop_lock:
            if block:
                while peers is None and hash in self._get_peer_loop_lock:
                    peers = self._get_peers(hash, compact=False)
                    self.sleep(1)
            return peers
        else:
            self._get_peer_loop_lock[hash]=time.time()
            self.debug(2, "get peers hash %s" % hash.encode("hex"))
            self.root.register_torrent(hash)
            tried_nodes = set()
            ts = time.time() + delay
            closest = self.get_closest_nodes(hash)
            typ = "peers"
            heapq.heappush(self._get_peer_loop_list, (ts, hash, tried_nodes, closest, typ, callback, limit))
            if block:
                while peers is None and hash in self._get_peer_loop_lock:
                    peers = self._get_peers(hash, compact=False)
                    self.sleep(1)
            return peers

    def _get_peers_closest_loop(self):
        def on_stop(hash, typ):
            self.root.release_torrent(hash)
            if typ == "peers":
                try: del self._get_peer_loop_lock[hash]
                except KeyError: pass
            elif typ == "closest":
                try: del self._get_closest_loop_lock[hash]
                except KeyError: pass

        def stop():
            while self._get_peer_loop_list:
                (_, hash, _, _, typ, _, _) = heapq.heappop(self._get_peer_loop_list)
                on_stop(hash, typ)

        while True:
            tosleep = 1
            while self._get_peer_loop_list:
                if self.stoped:
                    stop()
                    return
                # fetch next hash to process
                (ts, hash, tried_nodes, closest, typ, callback, limit) = heapq.heappop(self._get_peer_loop_list)
                if typ not in ["peers", "closest"]:
                    raise ValueError("typ should not be %s" % typ)
                # if process time is in the past process it
                if ts <= time.time():
                    # get hash k closest node that have not been tried
                    _closest = self.get_closest_nodes(hash)
                    __closest = [node for node in _closest if node not in tried_nodes]
                    
                    if __closest:
                        node = __closest[0]
                        # send a get peer to the closest
                        node.get_peers(self, hash)
                        tried_nodes.add(node)
                        ts = time.time() + 1
                        # we search peers and we found as least limit of them
                        if (typ == "peers" and limit and hash in self._peers and self._peers[hash] and len(self._peers[hash])>=limit):
                            self.debug(2, "Hash %s find peers" % hash.encode("hex"))
                            if callback:
                                callback(self._get_peers(hash, compact=False))
                            on_stop(hash, typ)
                        # we search closest node and we don't find any closest
                        elif (typ == "closest" and closest == _closest):
                            self.debug(2, "Hash %s find nodes" % hash.encode("hex"))
                            if callback:
                                callback(_closest)
                            on_stop(hash, typ)
                        # Else had it the the heap to be processed later
                        else:
                            heapq.heappush(self._get_peer_loop_list, (ts, hash, tried_nodes, _closest, typ, callback, limit))
                        del node
                        del ts
                    else:
                        # we search peers, and we found some
                        if (typ == "peers" and hash in self._peers and self._peers[hash]):
                            self.debug(2, "Hash %s find peers" % hash.encode("hex"))
                            if callback:
                                callback(self._get_peers(hash, compact=False))
                            on_stop(hash, typ)
                        # we did not found peers nor closest node althougth we ask every close nodes we know of
                        else:
                            self.debug(2, "Hash %s not peers or nodes not found" % hash.encode("hex"))
                            if callback:
                                callback([])
                            on_stop(hash, typ)
                    del _closest
                    del __closest
                else:
                    # if fetch time in the future, sleep until that date
                    tosleep = max(1, ts - time.time())
                    heapq.heappush(self._get_peer_loop_list, (ts, hash, tried_nodes, closest, typ, callback, limit))
                    break
                del tried_nodes
                del closest
            self.sleep(tosleep, stop)

    def _get_peers(self, info_hash, compact=True):
        """Return peers store locally by remote announce_peer"""
        if not info_hash in self._peers:
            return None
        else:
           peers = [(-t,ip,port) for ((ip, port), t) in self._peers[info_hash].items()]
           # putting the more recent annonces in first
           peers.sort()
           if compact:
               return [struct.pack("!4sH", socket.inet_aton(ip), port) for (_, ip, port) in peers[0:50]]
           else:
               return [(ip, port) for (_, ip, port) in peers[0:50]]

    def get_closest_nodes(self, id):
        return list(self.root.get_closest_nodes(id))
    
    def bootstarp(self):
        self.debug(0,"Bootstraping")
        find_node1=FindNodeQuery("", self.myid, self.myid)
        find_node2=FindNodeQuery("", self.myid, self.myid)
        find_node3=FindNodeQuery("", self.myid, self.myid)
        _, find_node1 = self._get_transaction_id(FindNodeResponse, find_node1)
        _, find_node2 = self._get_transaction_id(FindNodeResponse, find_node2)
        _, find_node3 = self._get_transaction_id(FindNodeResponse, find_node3)
        self.sendto(str(find_node1), ("router.utorrent.com", 6881))
        self.sendto(str(find_node2), ("genua.fr", 6880))
        self.sendto(str(find_node3), ("dht.transmissionbt.com", 6881))



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
                return
            try:
                (msg, addr) = self._to_send.get(timeout=1)
                while True:
                    if self.stoped:
                        return
                    try:
                        (_,sockets,_) = select.select([], [self.sock], [], 1)
                        if sockets:
                            self.sock.sendto(msg, addr)
                            self.socket_out+=1
                            break
                    except socket.error as e:
                        if e.errno not in [11, 1]: # 11: Resource temporarily unavailable
                            self.debug(0, "send:%r" %e )
                            raise
            except Queue.Empty:
                pass

    def sendto(self, msg, addr):
        self._to_send.put((msg, addr))

    def _recv_loop(self):
        while True:
            if self.stoped:
                return
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
                    if e.errno not in [11, 1]: # 11: Resource temporarily unavailable
                        self.debug(0, "send:%r : (%r, %r)" % (e, data, addr))
                        raise

                
    def _get_transaction_id(self, reponse_type, query, id_len=4):
        id = random(id_len)
        if id in self.transaction_type:
            return self._get_transaction_id(reponse_type, query, id_len=id_len+1)
        self.transaction_type[id] = (reponse_type, time.time(), query)
        query.t = id
        return (id, query)

    def _get_token(self, ip):
        """Generate a token for `ip`"""
        if ip in self.token and self.token[ip][-1][1] < 300:
            #self.token[ip] = (self.token[ip][0], time.time())
            return self.token[ip][-1][0]
        else:
            id = random(4)
            self.token[ip].append((id, time.time()))
            return id

    def _get_valid_token(self, ip):
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

        for id in self.transaction_type.keys():
            if now - self.transaction_type[id][1] > 30:
                del self.transaction_type[id]

        self._threads = [t for t in self._threads[:] if t.is_alive()]

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
            for hash, peers in self._peers.items():
                for peer in peers.keys():
                    if now - self._peers[hash][peer] > 15 * 60:
                        del self._peers[hash][peer]
                if not self._peers[hash]:
                    del self._peers[hash]

            self.clean_long()

            self.long_clean = now

    def build_table(self):
        nodes = self.get_closest_nodes(self.myid)
        for node in nodes:
            node.find_node(self, self.myid)
        return bool(nodes)

    def _routine(self):
        next_routine = time.time() + 15
        while True:
            if self.stoped:
                return
            self.sleep(next_routine - time.time())
            now = time.time()
            next_routine = now + 15

            # calling clean every 15s
            self._clean()

            # Searching its own id while the Routing table is growing
            if self.root_heigth != self.root.heigth():
                self.debug(1, "Fetching my own id")
                if self.build_table():
                    self.root_heigth += 1

            # displaying some stats
            (in_s, out_s, delta) = self.socket_stats()
            if in_s <= 0 or self.debuglvl > 0:
                (nodes, goods, bads) = self.root.stats()
                if goods <= 0:
                    self.bootstarp()
                self.debug(0 if in_s <= 0 and out_s > 0 and goods < 20 else 1, "%d nodes, %d goods, %d bads | in: %s, out: %s en %ss" % (nodes, goods, bads, in_s, out_s, int(delta)))
                if in_s <= 0 and out_s > 0 and self.last_msg_list:
                    self.debug(0, "\n".join("%r" % o for o in self.last_msg_list))
            self.last_msg_list = []



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
    def _on_ping_response(self, query, response):
        pass
    def _on_find_node_response(self, query, response):
        for node in response.r.get("nodes", []):
            self.root.add(self, node)
        self.debug(2, "%s nodes added to routing table" % len(response.r.get("nodes", [])))
    def _on_get_peers_response(self, query, response):
        self.mytoken[response["id"]]=(response["token"], time.time())
        for node in response.r.get("nodes", []):
            self.root.add(self, node)
        for ipport in response.r.get("values", []):
            (ip, port) = struct.unpack("!4sH", ipport)
            ip = socket.inet_ntoa(ip)
            self._add_peer(query.a["info_hash"], ip=ip, port=port)
    def _on_announce_peer_response(self, query, response):
        pass

    def _on_ping_query(self, query):
        pass
    def _on_find_node_query(self, query):
        pass
    def _on_get_peers_query(self, query):
        pass
    def _on_announce_peer_query(self, query):
        if query.a.get("implied_port", 0) != 0:
            self._add_peer(info_hash=query.a["info_hash"], ip=query.addr[0], port=query.addr[1])
        else:
            self._add_peer(info_hash=query.a["info_hash"], ip=query.addr[0], port=query.a["port"])


    def _process_response(self, obj, query):
        getattr(self, '_on_%s_response' % obj.q)(query, obj)
        getattr(self, 'on_%s_response' % obj.q)(query, obj)
    def _process_query(self, obj):
        getattr(self, '_on_%s_query' % obj.q)(obj)
        getattr(self, 'on_%s_query' % obj.q)(obj)

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



class BucketFull(Exception):
    pass

class NoTokenError(Exception):
    pass

@total_ordering
class Node(object):
    __slot__ = ("id", "ip", "port", "last_response", "last_query", "failed")
    def __init__(self, id, ip, port, last_response=0, last_query=0, failed=0):
        if not port > 0 and port < 65536:
            raise ValueError("Invalid port number %s, sould be within 1 and 65535" % port)
        self.id = id
        self.ip = ip
        self.port = port
        self.last_response = last_response
        self.last_query = last_query
        self.failed = failed


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
        t, msg = dht._get_transaction_id(PingResponse, msg)
        self.failed+=1
        dht.sendto(str(msg), (self.ip, self.port))

    def find_node(self, dht, target):
        msg = FindNodeQuery("", dht.myid, target)
        t, msg = dht._get_transaction_id(FindNodeResponse, msg)
        self.failed+=1
        dht.sendto(str(msg), (self.ip, self.port))

    def get_peers(self, dht, info_hash):
        msg = GetPeersQuery("", dht.myid, info_hash, )
        t, msg = dht._get_transaction_id(GetPeersResponse, msg)
        self.failed+=1
        dht.sendto(str(msg), (self.ip, self.port))

    def announce_peer(self, dht, info_hash, port):
        if self.id in dht.mytoken and (time.time() - dht.mytoken[self.id][1]) < 600:
            token = dht.mytoken[self.id][0]
            msg = AnnouncePeerQuery("", dht.myid, info_hash, port, token)
            t, msg = dht._get_transaction_id(AnnouncePeerResponse, msg)
            self.failed+=1
            dht.sendto(str(msg), (self.ip, self.port))

        else:
            raise NoTokenError()

class Bucket(list):
    max_size = 8
    last_changed = 0

    __slot__ = ("id", "id_length")

    def own(self, id):
        if id.startswith(self.id[:self.id_length/8]):
            for i in range(self.id_length/8*8, self.id_length):
                if nbit(self.id, i) !=  nbit(id, i):
                    return False
            return True
        else:
            return False

    def __init__(self, id="", id_length=0, init=None):
        self.id = id
        self.id_length = id_length # en bit
        if init:
            super(Bucket, self).__init__(init)

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

    def split(self, rt, dht):
        if self.id_length < 8*len(self.id):
            new_id = self.id
        else:
            new_id = self.id + "\0"
        b1 = Bucket(id=new_id, id_length=self.id_length + 1)
        b2 = Bucket(id=nflip(new_id, self.id_length), id_length=self.id_length + 1)
        for node in self:
            try:
                if b1.own(node.id):
                    b1.add(dht, node)
                else:
                    b2.add(dht, node)
            except BucketFull:
                rt.add(dht, node)
        if nbit(b1.id, self.id_length) == 0:
            return (b1, b2)
        else:
            return (b2, b1)

    def merge(self, bucket):
        l = [n for l in zip(self, bucket) for n in l if n.good][:self.max_size]
        return Bucket(id=self.id, id_length=self.id_length - 1, init=l)

    @property
    def to_refresh(self):
        return time.time() - self.last_changed > 15 * 60

class NotFound(Exception):
    pass

class RoutingTable(object):

    #__slot__ = ("trie", "_heigth", "split_ids", "info_hash", "last_merge", "lock", "_dhts", "stoped")
    def __init__(self, bucket=None, debuglvl=0):
        self.debuglvl = debuglvl
        self.trie = datrie.Trie(u"01")
        self.trie[u""]=Bucket()
        self._heigth=1
        self.split_ids = set()
        self.info_hash = set()
        #self.last_merge = 0
        self.lock = Lock()
        self._to_split = Queue.Queue()
        self._dhts = set()
        self.stoped = True
        self.need_merge = False
        self._threads = []
        self.threads = []
        self._to_merge = set()
        self._threads_zombie= []
        self.zombie = False

    def stop_bg(self):
        if not self.stoped:
            Thread(target=self.stop).start()

    def stop(self):
        if self.stoped:
            self.debug(0, "Already stoped or soping in progress")
            return
        self.stoped = True
        self._threads = [t for t in self._threads[:] if t.is_alive()]
        #self.debug(0, "Trying to terminate thread for 1 minutes")
        for i in range(0, 60):
            if self._threads:
                if i > 3:
                    self.debug(0, "Waiting for %s threads to terminate" % len(self._threads))
                time.sleep(1)
                self._threads = [t for t in self._threads[:] if t.is_alive()]
            else:
                break
        if self._threads:
            self.debug(0, "Unable to stop %s threads, giving up" % len(self._threads))
            self.zombie = True
            self._threads_zombie.extend(self._threads) 
            self._threads = []
        
    def start(self):
        with self.lock:
            if not self.stoped:
                self.debug(0, "Already started")
                return
            if self.zombie:
                self.debug(0, "Zombie threads, unable de start")
                return self._threads_zombie
            self.stoped = False

        for f in [self._merge_loop, self._routine, self._split_loop]:
            t = Thread(target=f)
            t.daemon = True
            t.start()
            self._threads.append(t)
            self.threads.append(t)

    def is_alive(self):
        if self.threads and reduce(lambda x,y: x and y, [t.is_alive() for t in self.threads]):
            return True
        elif not self._threads and self.stoped:
            return False
        else:
            self.debug(0, "One thread died, stopping dht")
            self.stop_bg()
            return True

    def register_torrent(self, id):
        self.info_hash.add(id)

    def release_torrent(self, id):
        try:
            self.info_hash.remove(id)
            try:
                to_merge = True
                key = self.trie.longest_prefix(self._ides(id))
                self._to_merge.add(key)
            except KeyError:
                pass
            if not self.need_merge:
                self.debug(1, "Programming merge")
                self.need_merge = True
        except KeyError:
            pass

    def _merge_loop(self):
        next_merge = 0
        # at most one full merge every 10 minutes
        next_full_merge = time.time() + 10 * 60
        while True:
            self.sleep(max(next_merge - time.time(), 1))
            if self._to_merge:
                stack = []
                while self._to_merge:
                    stack.append(self._to_merge.pop())
                next_merge = time.time() + 60
                self.debug(1, "Merging %s buckets" % (len(stack),))
                self._merge(stack)

            if self.need_merge and time.time() > next_full_merge:
                self.need_merge = False
                next_merge = time.time() + 60
                next_full_merge = time.time() + 10 * 60
                self._merge()

    def register_dht(self, dht):
        self._dhts.add(dht)
        self.split_ids.add(dht.myid)

    def release_dht(self, dht):
        try: self._dhts.remove(dht)
        except KeyError:pass
        try: 
            self.split_ids.remove(dht.myid)
            if not self.need_merge:
                self.debug(1, "Programming merge")
                self.need_merge = True
        except KeyError:
            pass

    def sleep(self, t, fstop=None):
        if t > 0:
            t_int = int(t)
            t_dec = t - t_int
            for i in range(0, t_int):
                time.sleep(1)
                if self.stoped:
                    if fstop:
                        fstop()
                    sys.exit(0)
            time.sleep(t_dec)

    def save(self):
        myid = str(self.myid).encode("hex")
        pickle.dump(self.root, open("dht_%s.status" % myid, 'w+'))

    def debug(self, lvl, msg):
        if lvl <= self.debuglvl:
            print("RT:%s" % msg)

    def _routine(self):
        last_explore_tree = 0
        while True:
            #self.clean()
            # exploring the routing table
            self.sleep(60 - (time.time() - last_explore_tree))
            dhts = list(self._dhts)
            shuffle(dhts)
            now = time.time()
            for key, bucket in self.trie.items():
                if self.stoped:
                    return
                # if trie modifies while looping
                if not key in self.trie:
                    continue
                # If bucket inactif for more than 15min, find_node on a random id in it
                if now - bucket.last_changed > 15 * 60:
                    id = bucket.random_id()
                    nodes = self.get_closest_nodes(id)
                    if nodes:
                        nodes[0].find_node(dhts[0], id)
                    del nodes
                # If questionnable nodes, ping one of them
                questionable = [node for node in bucket if not node.good and not node.bad]
                
                for dht in dhts:
                    if not questionable:
                        break
                    questionable.pop().ping(dht)
                del questionable

            last_explore_tree = time.time()

    def _split_loop(self):
        while True:
            if self.stoped:
                return
            try:
                (dht, id, callback) = self._to_split.get(timeout=1)
                self._split(dht, id, callback)
            except Queue.Empty:
                pass

    def split(self, dht, id, callback=None):
        self._to_split.put((dht, id, callback))


    def empty(self):
        """Remove all subtree"""
        self.trie = datrie.Trie("".join(chr(i) for i in range(256)))
        self.trie[u""]=Bucket()

    def stats(self):
        nodes = 0
        goods = 0
        bads = 0
        others = 0
        try:
            for b in self.trie.values():
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

    def __iter__(self):
        return iter(self.trie.values())

    def get_node(self, id):
        b = self.find(id)
        return b.get_node(id)

    def _ides(self, id):
        return u"{0:0160b}".format(int(str(id).encode("hex"), 16))

    #def _esif(self, id):
    #    id = id + u'0'* (160 - len(id))
    #    return ("%x" % int(id, 2)).decode("hex")

    def find(self, id):
        try:
            return self.trie.longest_prefix_value(self._ides(id))
        except KeyError:
            return self.trie[u""]

    def get_closest_nodes(self, id, errno=0):
        try:
            id = ID(id)
            nodes = set(n for n in self.find(id) if not n.bad)
            try:
                prefix = self.trie.longest_prefix(self._ides(id))
            except KeyError:
                prefix = u""
            while len(nodes) < Bucket.max_size and prefix:
                prefix = prefix[:-1]
                for suffix in self.trie.suffixes(prefix):
                    nodes = nodes.union(n for n in self.trie[prefix + suffix] if not n.bad)
            nodes = list(nodes)
            nodes.sort(key=lambda x:id ^ x.id)
            return nodes[0:Bucket.max_size]
        except KeyError as e:
            if errno>0:
                self.debug(1, "get_closest_nodes:%r" % e)
            return self.get_closest_nodes(id, errno=errno+1)

    def add(self, dht, node):
        b = self.find(node.id)
        try:
            b.add(dht, node)
        except BucketFull:
            for id in self.split_ids | self.info_hash:
                if b.own(id):
                    self.split(dht, node.id, callback=(self.add, (dht, node)))
                    return

    def heigth(self):
        return self._heigth

    def _split(self, dht, id, callback=None):
        #with self.lock:
        try:
            try:
                prefix = self.trie.longest_prefix(self._ides(id))
            except KeyError:
                if u"" in self.trie:
                    prefix = u""
                else:
                    return
            (zero_b, one_b) = self.trie[prefix].split(self, dht)
            self.trie[prefix + u"1"] = one_b
            self.trie[prefix + u"0"] = zero_b
            self._heigth = max(self._heigth, len(prefix) + 2)
            del self.trie[prefix]
        except KeyError:
            self.debug(0, "trie changed while splitting")
        if callback:
            callback[0](*callback[1])


    def merge(self):
        self.need_merge = True

    def _merge(self, stack=None):
        if stack is None:
            stack = self.trie.keys()
            full_merge = True
        else:
            full_merge = False
        if full_merge:
            nodes_before = self.stats()[0]
            if nodes_before < 1000:
                self.debug(1, "Less than 1000 nodes, no merge")
                return 
            started = time.time()
        while stack:
            if self.stoped:
                return
            key = stack.pop()
            if not key:
                continue
            to_merge =  True
            for id in self.split_ids | self.info_hash:
                if self._ides(id).startswith(key[:-1]):
                    to_merge = False
                    break
            if to_merge:
                #with self.lock:
                try:
                    if key not in self.trie:
                        self.debug(2, "%s gone away while merging" % key)
                        continue
                    prefix0 = key
                    prefix1 = key[:-1] + unicode(int(key[-1]) ^ 1)
                    bucket0 = self.trie[prefix0]
                    if prefix1 in self.trie:
                        bucket1 = self.trie[prefix1]
                        bucket = bucket0.merge(bucket1)
                        self.trie[key[:-1]] = bucket
                        del self.trie[prefix1]
                    else:
                        self.trie[key[:-1]] = Bucket(id=bucket0.id, id_length=len(key[:-1]), init=bucket0)
                    del self.trie[prefix0]
                    stack.append(key[:-1])
                except KeyError:
                    self.debug(0, "trie changed while merging")

        if full_merge:
            self._heigth = max(len(k) for k in self.trie.keys()) + 1
            self.debug(1, "%s nodes merged in %ss" % (nodes_before - self.stats()[0], int(time.time() - started)))
                


