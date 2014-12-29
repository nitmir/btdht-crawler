#!/usr/bin/env python
# -*- coding: utf-8 -*-
from functools import total_ordering
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
    def __init__(self, bind_port, bind_ip="0.0.0.0", root=None, id=None, ignored_ip=[]):
        self.myid = ID() if id is None else id
        self.root = BucketTree(bucket=Bucket(), split_ids=[self.myid]) if root is None else root
        if not self.myid in self.root.split_ids:
            self.root.split_ids.append(self.myid)
        self.transaction_type={}
        self.token={}
        self.mytoken={}
        self.peers={}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((bind_ip, bind_port))
        self.messages = Queue.Queue()
        self.root_heigth = 0
        self.last_routine = 0
        self.last_clean = time.time()
        self.ignored_ip = ignored_ip
        self.stop = False

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
        print "Bootstraping"
        find_node1=FindNodeQuery(self.get_transaction_id(FindNodeResponse), self.myid, self.myid)
        find_node2=FindNodeQuery(self.get_transaction_id(FindNodeResponse), self.myid, self.myid)
        find_node3=FindNodeQuery(self.get_transaction_id(FindNodeResponse), self.myid, self.myid)
        self.sock.sendto(str(find_node1), ("router.utorrent.com", 6881))
        self.sock.sendto(str(find_node2), ("genua.fr", 6880))
        self.sock.sendto(str(find_node3), ("dht.transmissionbt.com", 6881))

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

    def update_hash(self, info_hash, get):
        db = MySQLdb.connect(**config.mysql)
        cur = db.cursor()
        if get:
            cur.execute("INSERT INTO torrents (hash, dht_last_get) VALUES (%s,NOW()) ON DUPLICATE KEY UPDATE dht_last_get=NOW();",("".join("{:02x}".format(ord(c)) for c in info_hash),))
        else:
            cur.execute("INSERT INTO torrents (hash, dht_last_announce) VALUES (%s,NOW()) ON DUPLICATE KEY UPDATE dht_last_announce=NOW();",("".join("{:02x}".format(ord(c)) for c in info_hash),))
        db.commit()
        db.close()

    def loop(self):
        while True:
            if self.stop:
                return
            try:
                (sockets,_,_) = select.select([self.sock], [], [], 1)
            except KeyboardInterrupt:
                self.save()
                raise
            if sockets:
                data, addr = self.sock.recvfrom(4048)
                if addr in self.ignored_ip:
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
                        if isinstance(obj, GetPeersQuery) or isinstance(obj, AnnouncePeerQuery):
                            print "R:%r" % "".join("{:02x}".format(ord(c)) for c in obj["info_hash"])
                            #print "S:%r" % reponse
                        self.sock.sendto(str(reponse), addr)
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
                except BError as error:
                    self.sock.sendto(str(error), addr)
                except BcodeError:
                    self.sock.sendto(str(ProtocolError("", "malformed packet")), addr)
                except KeyboardInterrupt:
                    self.save()
                    raise

                #print self.root
            if time.time() - self.last_routine >= 5:
                self.last_routine = time.time()
                self.routine()
                    
                
    def get_transaction_id(self, reponse_type):
        id = random(4)
        if id in self.transaction_type:
            return self.get_transaction_id(reponse_type)
        self.transaction_type[id] = (reponse_type, time.time())
        return id

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
        if now - self.last_clean < 15 * 60:
            return
        self.save()
        for id in self.transaction_type.keys():
            if now - self.transaction_type[id][1] > 15 * 60:
                del self.transaction_type[id]
        self.last_clean = now

    def routine(self):
        now = time.time()
        self.clean()
        if self.root_heigth != self.root.heigth():
            nodes = self.get_closest_node(self.myid)
            if nodes:
                self.root_heigth += 1
            for node in nodes:
                node.find_node(self, self.myid)
        (nodes, goods, bads) = self.root.stats()
        if goods == 0:
            self.bootstarp()
        print "%d nodes, %d goods, %d bads" % (nodes, goods, bads)
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
            for nodes in obj.r.get("nodes", []):
                self.root.add(self, node)
            #print "R:%r" % obj
        elif isinstance(obj, AnnouncePeerResponse):
            #print "R:%r" % obj
            pass
        else:
            raise MethodUnknownError("", "%r" % obj)
    def decode(self, s):
        d = bdecode(s)
        if not isinstance(d, dict):
            raise ProtocolError("", "Message send is not a dict")
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
                    if "values" in d["r"]:
                        ret = GetPeersResponse(d["t"], d["r"]["id"], d["r"]["token"], values=d["r"]["values"])
                    elif "nodes" in d["r"]:
                        ret = GetPeersResponse(d["t"], d["r"]["id"], d["r"]["token"], nodes=Node.from_compact_infos(d["r"]["nodes"]))
                    else:
                        raise ProtocolError(d["t"], "get_peers responses should have a values key or a nodes key")
                elif ttype == AnnouncePeerResponse:
                    ret = AnnouncePeerResponse(d["t"], d["r"]["id"])
                else:
                    raise MethodUnknownError(d["t"], "Method unknown %s" % ttype.__name__)
                del self.transaction_type[d["t"]]
                return ret
            else:
                raise GenericError(d["t"], "transaction id unknown")
        elif d["y"] == "e":
            print "ERROR:%r" % d




class BucketFull(Exception):
    pass

class NoTokenError(Exception):
    pass

@total_ordering
class Node(object):
    def __init__(self, id, ip, port, conn=None):
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
            nodes.append(Node.from_compact_info(infos[i:i+26]))
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
        t = dht.get_transaction_id(PingResponse)
        msg = PingQuery(t, dht.myid)
        #print "S:%r" % msg
        self.failed+=1
        dht.sock.sendto(str(msg), (self.ip, self.port))

    def find_node(self, dht, target):
        t = dht.get_transaction_id(FindNodeResponse)
        msg = FindNodeQuery(t, dht.myid, target)
        #print "S:%r" % msg
        self.failed+=1
        dht.sock.sendto(str(msg), (self.ip, self.port))

    def get_peers(self, dht, info_hash):
        t = dht.get_transaction_id(GetPeersResponse)
        msg = GetPeersQuery(t, dht.myid, info_hash, )
        #print "S:%r" % msg
        self.failed+=1
        dht.sock.sendto(str(msg), (self.ip, self.port))

    def announce_peer(self, dht, info_hash, port):
        if self.id in dht.mytoken:
            t = dht.get_transaction_id(AnnouncePeerResponse)
            token = dht.mytoken[self.id]
            msg = AnnouncePeerQuery(t, dht.myid, info_hash, port, token)
            #print "S:%r" % msg
            self.failed+=1
            dht.sock.sendto(str(msg), (self.ip, self.port))
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
        id_end = id[self.id_length/8]
        tmp = ''
        if self.id_length>0:
            try:
               id_start = self.id[self.id_length/8]
            except IndexError:
                id_start = "\0"
            for i in range((self.id_length % 8)):
                tmp +=str(nbit(id_start, i))
        for i in range((self.id_length % 8), 8):
            tmp +=str(nbit(id_end, i))
        char = chr(int(tmp, 2))
        return ID(self.id[0:self.id_length/8] + char + id[self.id_length/8+1:])

    def get_node(self, id):
        for n in self:
            if n.id == id:
                return n
        raise NotFound()

    def add(self, dht, node):
        if not self.own(node.id):
            raise ValueError("Wrong Bucket")
        elif node in self:
            old_node = self.get_node(node.id)
            old_node.ip = node.ip
            old_node.port = node.port
            self.last_changed = time.time()
        elif len(self) < self.max_size:
            self.append(node)
            self.last_changed = time.time()
        else:
            for n in self:
                if n.bad:
                    self.remove(n)
                    self.add(dht, node)
                    return
            self.sort()
            if not self[-1].good:
                self[-1].ping(dht)
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

    def __init__(self, bucket=None, zero=None, one=None, parent=None, level=0, split_ids=[]):
        self.zero = zero
        self.one = one
        self.bucket = bucket
        self.level = level
        self.parent = parent
        self.split_ids=split_ids

    def stats(self):
        nodes = 0
        goods = 0
        bads = 0
        others = 0
        for b in self:
            for n in b:
                nodes+=1
                if n.good:
                    goods+=1
                elif n.bad:
                    bads+=1
                else:
                    others+=1
        return (nodes, goods, bads)

    def heigth(self):
        if self.bucket is None:
            return 1 + max(self.zero.heigth(), self.one.heigth())
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
            b = stack.pop()
            if b.bucket is None:
                stack.extend([b.zero, b.one])
            else:
                yield b.bucket

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
        return self._find(id).bucket

    def get_closest_nodes(self, id, bt=None, nodes=None, done=None):
        if not isinstance(id, ID):
            id = ID(id)
        if nodes is None:
            nodes = set()
        if done is None:
            done = set()
        if bt in done:
            return nodes
        if len(nodes) >= Bucket.max_size:
            return nodes
        if bt is None:
            bt = self._find(id)
        if bt.bucket is not None:
            for n in bt.bucket:
                if n.good:
                    nodes.add(n)
            done.add(bt)
            if bt.parent is not None:
                return self.get_closest_nodes(id, bt.parent, nodes, done)
            else:
                return nodes
        elif bt.one and bt.zero:
            nodes1 = self.get_closest_nodes(id, bt.one, nodes, done)
            nodes0 = self.get_closest_nodes(id, bt.zero, nodes, done)
            done.add(bt)
            return self.get_closest_nodes(id, bt.parent, nodes0.union(nodes1), done)
        else:
            raise EnvironmentError("bucket, zero and one are None")

    def add(self, dht, node):
        b = self.find(node.id)
        try:
            b.add(dht, node)
        except BucketFull:
            for id in self.split_ids:
                if b.own(id):
                    self.split(dht, node.id)
                    self.add(dht, node)
                    return

    def split(self, dht, id):
        bt = self._find(id)
        (zero_b, one_b) = bt.bucket.split(dht)
        bt.zero = BucketTree(bucket=zero_b, parent=bt, level=bt.level+1, split_ids=self.split_ids)
        bt.one = BucketTree(bucket=one_b, parent=bt, level=bt.level+1, split_ids=self.split_ids)
        bt.bucket = None
        
class RoutingTable(object):
    root = BucketTree(bucket=Bucket())
    def __init__(self, (boostrap_ip, boostrap_port)):
        pass



id1 = ID('\x8c\xc4[\xb1\xae\x8c\x8b\x00\x98dz\xd7%\xc3\x12\xda\xc4iSl')
dht1 = DHT(bind_port=12345, id=id1, ignored_ip=["188.165.207.160", "127.0.0.1", "10.8.0.1", "10.9.0.1", "192.168.10.1", "192.168.10.100", "192.168.10.101"])

dht1.load()

id2 = ID(id1 ^ utils.nflip("\0"*20, 0))
dht2 = DHT(bind_port=12370, id=id2, root=dht1.root, ignored_ip=["188.165.207.160", "127.0.0.1", "10.8.0.1", "10.9.0.1", "192.168.10.1", "192.168.10.100", "192.168.10.101"])

id3 = ID(id1 ^ utils.nflip("\0"*20, 1))
dht3 = DHT(bind_port=12371, id=id3, root=dht1.root, ignored_ip=["188.165.207.160", "127.0.0.1", "10.8.0.1", "10.9.0.1", "192.168.10.1", "192.168.10.100", "192.168.10.101"])

id4 = ID(id2 ^ utils.nflip("\0"*20, 1))
dht4 = DHT(bind_port=12372, id=id4, root=dht1.root, ignored_ip=["188.165.207.160", "127.0.0.1", "10.8.0.1", "10.9.0.1", "192.168.10.1", "192.168.10.100", "192.168.10.101"])


dhts = [dht1, dht2, dht3, dht4]

def lauch():
    ts = []
    for dht in dhts:
        ts.append(Thread(target=dht.loop))
        dht.stop = False
    for t in ts:
        t.start()
    try:
        while True:
            for t in ts:
                if not t.is_alive():
                    raise Exception("Stoped")
            time.sleep(1)
    except:
        stop()
        raise

def stop():
    for dht in dhts:
        dht.stop = True
    dht1.save()
