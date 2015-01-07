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

import config
from utils import *
from krcp import *

class DHT(object):
    def __init__(self, bind_port, bind_ip="0.0.0.0", root=None, id=None, ignored_ip=[], debuglvl=0, prefix=""):
        self.myid = ID() if id is None else id
        self.root = BucketTree(bucket=Bucket(), split_ids=[self.myid]) if root is None else root
        if not self.myid in self.root.split_ids:
            self.root.split_ids.append(self.myid)
        self.root.info_hash = []
        self.transaction_type={}
        self.token={}
        self.mytoken={}
        self.peers={}
        self.bind_port = bind_port
        self.bind_ip = bind_ip
        self.sock = None
        self.messages = Queue.Queue()
        self.root_heigth = 0
        self.last_routine = 0
        self.last_clean = time.time()
        self.ignored_ip = ignored_ip
        self.stop = False
        self.root.last_merge = 0
        self.db = None
        self.socket_in = 0
        self.socket_out = 0
        self.last_socket_stats = time.time()
        self.debuglvl = debuglvl
        self.last_msg = time.time()
        self.last_msg_rep = time.time()
        self.last_msg_list = []
        self.long_clean = time.time()
        self.prefix = prefix

        self.init_socket()

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

    def determine_info_hash(self, hash):
        if hash in self.root.good_info_hash or hash in self.root.bad_info_hash or hash in self.root.unknown_info_hash:
            return
        else:
            self.root.unknown_info_hash[hash]=time.time()
            self.debug(1, "Determining hash %s" % format_hash(hash))
            if not hash in self.root.info_hash:
                self.root.info_hash.append(hash)
            tried_nodes = set()
            time.sleep(15)
            closest = [node for node in self.get_closest_node(hash) if node not in tried_nodes]
            while closest:
                node = closest[0]
                #for node in closest:
                try:
                    node.get_peers(self, hash)
                except socket.error as e:
                    print "%s%r %r" % (self.prefix, e, (node.ip, node.port))
                tried_nodes.add(node)
                time.sleep(5)
                if hash in self.root.good_info_hash or self.stop:
                    self.debug(1, "Hash %s is good" % format_hash(hash))
                    self.root.info_hash.remove(hash)
                    return
                closest = [node for node in self.get_closest_node(hash) if node not in tried_nodes]
            self.debug(1, "Hash %s is bad" % format_hash(hash))
            self.root.bad_info_hash[hash]=time.time()
            self.root.info_hash.remove(hash)
            return
            

    def add_peer(self, info_hash, ip, port):
        if not info_hash in self.peers:
            self.peers[info_hash]={}
        self.peers[info_hash][(ip,port)]=time.time()

    def get_peers(self, info_hash):
        if not info_hash in self.peers:
            return None
        else:
           peers = [(t,ip,port) for ((ip, port), t) in self.peers[info_hash].items()]
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
        self.sock.sendto(str(find_node1), ("router.utorrent.com", 6881))
        self.sock.sendto(str(find_node2), ("genua.fr", 6880))
        self.sock.sendto(str(find_node3), ("dht.transmissionbt.com", 6881))
        self.socket_out+=3

    def save(self):
        myid = "".join("{:02x}".format(ord(c)) for c in self.myid)
        pickle.dump(self.root, open("dht_%s.status" % myid, 'w+'))

    def load(self, file=None):
        if file is None:
            myid = "".join("{:02x}".format(ord(c)) for c in self.myid)
            file = "dht_%s.status" % myid
        try:
            self.root = pickle.load(open(file))
        except IOError:
            self.bootstarp()
        self.root.info_hash = []

    def update_hash(self, info_hash, get, errornb=0):
        if len(info_hash) != 20:
            raise ProtocolError("", "info_hash should by 20B long")
        if self.db is None:
            self.db = MySQLdb.connect(**config.mysql)
        try:
            cur = self.db.cursor()
            if get:
                cur.execute("INSERT INTO torrents (hash, visible_status, dht_last_get) VALUES (%s,2,NOW()) ON DUPLICATE KEY UPDATE dht_last_get=NOW();",("".join("{:02x}".format(ord(c)) for c in info_hash),))
            else:
                cur.execute("INSERT INTO torrents (hash, visible_status, dht_last_announce) VALUES (%s,2,NOW()) ON DUPLICATE KEY UPDATE dht_last_announce=NOW();",("".join("{:02x}".format(ord(c)) for c in info_hash),))
            self.db.commit()
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

    def loop(self):
        while True:
            if self.stop:
                return
            try:
                (sockets,_,_) = select.select([self.sock], [], [], 1)
            except KeyboardInterrupt:
                #self.save()
                raise
            except socket.error as e:
                self.debug(0, "%r" %e )
                self.init_socket()

            if sockets:
                try:
                    data, addr = self.sock.recvfrom(4048)
                except socket.error as e:
                    self.debug(0, "%r : (%r, %r)" % (e, data, addr))
                    continue
                if addr in self.ignored_ip:
                    continue
                if addr[1] < 1 or addr[1] > 65535:
                    self.debug(1, "Port should be whithin 1 and 65535, not %s" % addr[1])
                    continue
                try:
                    obj = self.decode(data)
                    if isinstance(obj, BQuery):
                        try:
                            node = self.root.get_node(obj["id"])
                            node.last_query = time.time()
                            node.ip = addr[0]
                            node.port = addr[1]
                        except NotFound:
                            node = Node(id=obj["id"], ip=addr[0], port=addr[1])
                            node.last_query = time.time()
                            self.root.add(self, node)
                        reponse = obj.response(self, ip=addr[0])
                        #if isinstance(obj, GetPeersQuery) or isinstance(obj, AnnouncePeerQuery):
                            #print "R:%r from %s" % ("".join("{:02x}".format(ord(c)) for c in obj["info_hash"]), addr[0])
                            #print "S:%r" % reponse

                        self.socket_in+=1
                        self.last_msg = time.time()
                        self.last_msg_list.append(obj)

                        self.sock.sendto(str(reponse), addr)
                        self.socket_out+=1
                    elif isinstance(obj, BResponse):
                        try:
                            node = self.root.get_node(obj["id"])
                            node.last_response = time.time()
                            node.ip = addr[0]
                            node.port = addr[1]
                            node.failed = 0
                        except NotFound:
                            node = Node(id=obj["id"], ip=addr[0], port=addr[1])
                            node.last_response = time.time()
                            self.root.add(self, node)
                        self.process_response(obj)

                        self.socket_in+=1
                        self.last_msg = time.time()
                        self.last_msg_rep = time.time()
                        self.last_msg_list.append(obj)

                except BError as error:
                    self.sock.sendto(str(error), addr)
                    self.socket_out+=1
                except BcodeError:
                    self.sock.sendto(str(ProtocolError("", "malformed packet")), addr)
                    self.socket_out+=1
                except socket.error as e:
                    self.debug(0, "%r : (%r, %r)" % (e, data, addr))
                    #self.init_socket()
                except KeyboardInterrupt:
                    #self.save()
                    raise

                #print self.root
            if time.time() - self.last_routine >= 10:
                self.last_routine = time.time()
                try:
                    self.routine()
                except socket.error as e :
                    self.debug(0, "%r" % e)
                    
                
    def get_transaction_id(self, reponse_type, query):
        id = random(2)
        if id in self.transaction_type:
            return self.get_transaction_id(reponse_type, query)
        self.transaction_type[id] = (reponse_type, time.time(), query)
        query.t = id
        return (id, query)

    def get_token(self, ip):
        if ip in self.token:
            self.token[ip] = (self.token[ip][0], time.time())
            return self.token[ip][0]
        else:
            id = random(4)
            self.token[ip] = (id, time.time())
            return id

    def clean(self):
        now = time.time()
        if now - self.last_clean < 15:
            return
        #self.save()
        for id in self.transaction_type.keys():
            if now - self.transaction_type[id][1] > 30:
                del self.transaction_type[id]

        if now - self.last_msg > 2 * 60:
            self.debug(0, "No msg since more then 2 minutes")
            try:self.sock.close()
            except: pass
            self.stop = True
        elif now - self.last_msg_rep > 5 * 60:
            self.debug(0, "No msg response since more then 5 minutes")
            try:self.sock.close()
            except: pass
            self.stop = True

        # Long cleaning
        if now - self.long_clean >= 15 * 60:
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
            if now - self.root.last_merge > 15 * 60:
                self.root.last_merge = now
                Thread(target=self.root.merge, args=(self,)).start()
            self.long_clean = now

        self.last_clean = now

    def routine(self):
        now = time.time()
        self.clean()
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
        for bucket in iter(self.root):
            if now - bucket.last_changed > 15 * 60:
                id = bucket.random_id()
            else:
                id = None
            questionable = [node for node in bucket if not node.good and not node.bad]
            good = [node for node in bucket if node.good]
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

    def process_response(self, obj):
        if isinstance(obj, PingResponse):
            pass
        elif isinstance(obj, FindNodeResponse):
            for node in obj["nodes"]:
                self.root.add(self, node)
        elif isinstance(obj, GetPeersResponse):
            self.mytoken[obj["id"]]=obj["token"]
            for node in obj.r.get("nodes", []):
                self.root.add(self, node)
            if obj.r.get("values", []):
                info_hash = self.transaction_type[obj.t][2]["info_hash"]
                self.root.good_info_hash[info_hash]=time.time()
                if info_hash in self.root.bad_info_hash:
                    del self.root.bad_info_hash[info_hash]
                if info_hash in self.root.unknown_info_hash:
                    del self.root.unknown_info_hash[info_hash]
                self.update_hash(info_hash, get=False)
            #print "R:%r" % obj
        elif isinstance(obj, AnnouncePeerResponse):
            info_hash = self.transaction_type[obj.t][2]["info_hash"]
            self.root.good_info_hash[info_hash]=time.time()
            if info_hash in self.root.bad_info_hash:
                del self.root.bad_info_hash[info_hash]
            if info_hash in self.root.unknown_info_hash:
                del self.root.unknown_info_hash[info_hash]
            self.update_hash(info_hash, get=False)
            #print "R:%r" % obj
            pass
        else:
            raise MethodUnknownError("", "%r" % obj)
    def decode(self, s):
        d = bdecode(s)
        if not isinstance(d, dict):
            raise ProtocolError("", "Message send is not a dict")
        if not "y" in d:
            raise ProtocolError("", "Message malformed: y key is missing")
        if not "t" in d:
            raise ProtocolError("", "Message malformed: t key is mandatory")
        if d["y"] == "q":
            if d["q"] == "ping":
                return PingQuery(d["t"], d["a"]["id"])
            elif d["q"] == "find_node":
                return FindNodeQuery(d["t"], d["a"]["id"], d["a"]["target"])
            elif d["q"] == "get_peers":
                return GetPeersQuery(d["t"], d["a"]["id"], d["a"]["info_hash"])
            elif d["q"] == "announce_peer":
                return AnnouncePeerQuery(d["t"], d["a"]["id"], d["a"]["info_hash"], d["a"]["port"], d["a"]["token"], d["a"].get("implied_port", None))
            else:
                raise MethodUnknownError(d["t"], "Method %s is unknown" % d["q"])
        elif d["y"] == "r":
            if d["t"] in self.transaction_type:
                ttype = self.transaction_type[d["t"]][0]
                if ttype == PingResponse:
                    ret = PingResponse(d["t"], d["r"]["id"])
                elif ttype == FindNodeResponse:
                    ret = FindNodeResponse(d["t"], d["r"]["id"], Node.from_compact_infos(d["r"].get("nodes", "")))
                elif ttype == GetPeersResponse:
                    if "values" in d["r"] and "token" in d["r"]:
                        ret = GetPeersResponse(d["t"], d["r"]["id"], d["r"]["token"], values=d["r"]["values"])
                    elif "nodes" in d["r"] and "token" in d["r"]:
                        ret = GetPeersResponse(d["t"], d["r"]["id"], d["r"]["token"], nodes=Node.from_compact_infos(d["r"]["nodes"]))
                    else:
                        raise ProtocolError(d["t"], "get_peers responses should have a values key or a nodes key")
                elif ttype == AnnouncePeerResponse:
                    ret = AnnouncePeerResponse(d["t"], d["r"]["id"])
                else:
                    raise MethodUnknownError(d["t"], "Method unknown %s" % ttype.__name__)
                return ret
            else:
                raise GenericError(d["t"], "transaction id unknown")
        elif d["y"] == "e":
            self.debug(2, "ERROR:%r pour %r" % (d, self.transaction_type.get(d["t"], {})))
        else:
            self.debug(0, "UNKNOWN MSG: %r" % d)
            raise ProtocolError(d["t"])




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
        #print "S:%r" % msg
        self.failed+=1
        dht.sock.sendto(str(msg), (self.ip, self.port))
        dht.socket_out+=1

    def find_node(self, dht, target):
        msg = FindNodeQuery("", dht.myid, target)
        t, msg = dht.get_transaction_id(FindNodeResponse, msg)
        #print "S:%r" % msg
        self.failed+=1
        dht.sock.sendto(str(msg), (self.ip, self.port))
        dht.socket_out+=1

    def get_peers(self, dht, info_hash):
        msg = GetPeersQuery("", dht.myid, info_hash, )
        t, msg = dht.get_transaction_id(GetPeersResponse, msg)
        #print "S:%r" % msg
        self.failed+=1
        dht.sock.sendto(str(msg), (self.ip, self.port))
        dht.socket_out+=1

    def announce_peer(self, dht, info_hash, port):
        if self.id in dht.mytoken:
            msg = AnnouncePeerQuery("", dht.myid, info_hash, port, token)
            t, msg = dht.get_transaction_id(AnnouncePeerResponse, msg)
            token = dht.mytoken[self.id]
            #print "S:%r" % msg
            self.failed+=1
            dht.sock.sendto(str(msg), (self.ip, self.port))
            dht.socket_out+=1
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
        self.good_info_hash = {}
        self.bad_info_hash = {}
        self.unknown_info_hash = {}

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
        if self.bucket is None:
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
ignored_ip = ["188.165.207.160", "10.8.0.1", "10.9.0.1", "192.168.10.1", "192.168.10.100", "192.168.10.101"]
port_base = 12345
prefix=1
dht_base = DHT(bind_port=port_base, id=id_base, ignored_ip=ignored_ip, debuglvl=debug, prefix="%s:" % prefix)
dht_base.load()
dhts = [dht_base]
for id in enumerate_ids(4, id_base):
    if id == id_base:
        continue
    prefix+=1
    dhts.append(DHT(bind_port=port_base + ord(id[0]), id=ID(id), root=dht_base.root, ignored_ip=ignored_ip, debuglvl=debug, prefix="%s:" % prefix))

thread_to_dht={}
dht_to_thread={}
def lauch():
    ts = []
    for dht in dhts:
        t=Thread(target=dht.loop)
        dht_to_thread[dht]=t
        thread_to_dht[t]=dht
        #ts.append(Thread(target=dht.loop))
        dht.stop = False
    try:
        for t in thread_to_dht:
            t.start()
            time.sleep(1.4142135623730951 * 2)
        while True:
            for t in thread_to_dht.keys():
                if not t.is_alive():
                    #print("thread stopped, restarting")
                    #dht = thread_to_dht[t]
                    #dht.stop = False
                    #del thread_to_dht[t]
                    #t = Thread(target=dht.loop)
                    #dht_to_thread[dht]=t
                    #thread_to_dht[t]=dht
                    #t.start()
                    raise Exception("Stoped")
            time.sleep(10)
    except Exception as e:
        print("%r" % e)
        stop()
        #try:
        #    stop()
        #    time.sleep(20)
        #finally:
        print("exit")
        os._exit(0)
        raise

def stop():
    for dht in dhts:
        dht.stop = True
    #dht1.save()


if __name__ == '__main__':
    lauch()
