# -*- coding: utf-8 -*-
import utils
from utils import ID 

import pyximport
pyximport.install()
from krcp_nogil import BErrorNG

class BMessage(object):

    dht_version = "DP\x00\x01" # dht python 1

    def _check_id(self, id):
        # little hack for some buggy dht client
        if isinstance(id, long):
            id = ('%040x' % id).decode("hex")
        if not isinstance(id, str) and not isinstance(id, ID):
            print "BAD ID %r of type %s %s" % (id, type(id).__name__, self.v)
            raise ProtocolError(self.t, "id and info_hash should be string")
        elif len(id) != 20:
            print "BAD ID %r of len %s %s" % (id.encode("hex"), len(id), self.v)
            raise ProtocolError(self.t, "id and info_hash should be 20 byte long")
        return id

    def _check_str(self, s):
        if not isinstance(s, str):
            raise ProtocolError(self.t, "String expected")

    def _check_int(self, i):
        if not isinstance(i, int):
            raise ProtocolError(self.t, "Integer expected")

class BQuery(BMessage):
    y = "q"
    t = None # string value representing a transaction ID (no more than 16b)
    q = None # string value containing the method name of the query
    a = None # dictionary value containing named arguments to the query
    r = None
    addr = None
    v = ""
    def _check_t(self, t):
        if self.t is not None:
            return
        if isinstance(t, dht.DHT):
            t._get_transaction_id(self.r, self)
        else:
            self.t = t

    def __init__(self, t, a, addr, v):
        self._check_t(t)
        # v is not None if message received from network
        if v is not None:
            self.v = v
        # else we are building a message
        else:
            self.v = self.dht_version
        self.addr = addr
        a["id"] = self._check_id(a["id"])
        self.a = a

    def __getitem__(self, key):
        return self.a[key]
    def __str__(self):
        return utils.bencode({"y":self.y, "t":self.t, "q":self.q, "a":self.a, "v":self.v})
    def __repr__(self):
        return repr({"y":self.y, "t":self.t.encode("hex"), "q":self.q, "a":self.a, "v":self.v})

class BResponse(BMessage):
    y = "r"
    t = None # string value representing a transaction ID
    r = None # dictionary containing named return values
    addr = None
    def __init__(self, t, r, addr, v):
        if isinstance(t, dht.DHT):
            raise ValueError("transaction id should already have been determined")
        self.t = t
        # v is not None if message received from network
        if v is not None:
            self.v = v
        # else we are building a message
        else:
            self.v = self.dht_version
        r["id"] = self._check_id(r["id"])
        self.r = r
        self.addr = addr
    def __getitem__(self, key):
        return self.r[key]
    def __str__(self):
        return utils.bencode({"y":self.y, "t":self.t, "r":self.r, "v":self.v})
    def __repr__(self):
        return repr({"y":self.y, "t":self.t.encode("hex"), "r":self.r, "v":self.v})

class PingResponse(BResponse):
    q = "ping"
    def __init__(self, t, id, addr=None, v=None):
        super(PingResponse, self).__init__(t, {"id" : id}, addr, v)

class FindNodeResponse(BResponse):
    q = "find_node"
    def __init__(self, t, id, nodes, addr=None, v=None):
        if not isinstance(nodes, list):
            raise ProtocolError(t, "nodes should be a list")
        super(FindNodeResponse, self).__init__(t, {"id" : ID(id), "nodes" : nodes}, addr, v)
    def __str__(self):
        return utils.bencode({"y":self.y, "t":self.t, "r":{"id" : self.r["id"], "nodes" : "".join((n.compact_info() for n in self.r["nodes"]))}})

class GetPeersResponse(BResponse):
    q = "get_peers"
    def __init__(self, t, id, token, values=None, nodes=None, addr=None, v=None):
        if nodes is not None:
            if not isinstance(nodes, list):
                raise ProtocolError(t, "nodes should be a list")
            super(GetPeersResponse, self).__init__(t, {"id" : ID(id), "token": ID(token), "nodes":nodes}, addr, v)
        elif values is not None:
            if not isinstance(values, list):
                raise ProtocolError(t, "values should be a list")
            for ipport in values:
                if not isinstance(ipport, str) or len(ipport) != 6:
                    raise ProtocolError(t, "values elements sould be strings of 6 bytes")
            super(GetPeersResponse, self).__init__(t, {"id" : ID(id), "token": ID(token), "values":values}, addr, v)
        else:
            raise ValueError("values or nodes needed")
    def __str__(self):
        if "nodes" in self.r:
            return utils.bencode({"y":self.y, "t":self.t, "r":{"id" : self.r["id"], "token" : self.r["token"], "nodes" : "".join((n.compact_info() for n in self.r["nodes"]))}})
        else:
            return super(GetPeersResponse, self).__str__()

class AnnouncePeerResponse(BResponse):
    q = "announce_peer"
    def __init__(self, t, id, addr=None, v=None):
        super(AnnouncePeerResponse, self).__init__(t, {"id" : ID(id)}, addr, v)


class PingQuery(BQuery):
    q = "ping"
    r = PingResponse
    def __init__(self, t, id, addr=None, v=None):
        super(PingQuery, self).__init__(t, {"id" : ID(id)}, addr, v)
    def response(self, dht, **kwargs):
        return self.r(self.t, dht.myid)

class FindNodeQuery(BQuery):
    q = "find_node"
    r = FindNodeResponse
    def __init__(self, t, id, target, addr=None, v=None):
        self._check_t(t)
        target = self._check_id(target)
        super(FindNodeQuery, self).__init__(t, {"id" : ID(id), "target" : ID(target)}, addr, v)
    def response(self, dht, **kwargs):
        return self.r(self.t, dht.myid, dht.get_closest_nodes(self.a["target"]))

class GetPeersQuery(BQuery):
    q = "get_peers"
    r = GetPeersResponse
    def __init__(self, t, id, info_hash, addr=None, v=None):
        self._check_t(t)
        info_hash = self._check_id(info_hash)
        super(GetPeersQuery, self).__init__(t, {"id" : ID(id), "info_hash" : ID(info_hash)}, addr, v)
    def response(self, dht, **kwargs):
        token = dht._get_token(self.addr[0])
        values = dht._get_peers(self.a["info_hash"])
        if values:
            return self.r(self.t, dht.myid, token, values=values)
        else:
            nodes = dht.get_closest_nodes(self.a["info_hash"])
            return self.r(self.t, dht.myid, token, nodes=nodes)

class AnnouncePeerQuery(BQuery):
    q = "announce_peer"
    r = AnnouncePeerResponse
    def __init__(self, t, id, info_hash, port, token, implied_port=None, addr=None, v=None):
        self._check_t(t)
        self._check_int(port)
        info_hash = self._check_id(info_hash)
        # If implied_port is not None and non-zero, the port argument should be ignored and the source port of the UDP packet should be used as the peer's port instead.
        if implied_port is not None:
            super(AnnouncePeerQuery, self).__init__(t, {"id" : ID(id), "info_hash" : ID(info_hash), "port" : port, "token" : ID(token), "implied_port" : implied_port}, addr, v)
        else:
            super(AnnouncePeerQuery, self).__init__(t, {"id" : ID(id), "info_hash" : ID(info_hash), "port" : port, "token" : ID(token)}, addr, v)
    def response(self, dht, **kwargs):
        if not self.a["token"] in dht._get_valid_token(self.addr[0]):
            raise ProtocolError("Bad token")
        return self.r(self.t, dht.myid)

class BError(BErrorNG):
    y = "e"
    t = None # string value representing a transaction ID
    e = None # a list. The first element is an integer representing the error code. The second element is a string containing the error message
    def __init__(self, t, e, **kwargs):
        if t is None:
            raise ValueError("t should not be None")
        self.t = t
        self.e = e
        super(BError, self).__init__(*e, **kwargs)
    def __str__(self):
        return utils.bencode({"y":self.y, "t":self.t, "e":self.e})
    def __repr__(self):
        return "%s: %s" % self.e

class GenericError(BError):
    def __init__(self, t, msg=""):
        super(GenericError, self).__init__(t=t, e=[201, msg])
class ServerError(BError):
    def __init__(self, t, msg="Server Error"):
        super(ServerError, self).__init__(t=t, e=[202, msg])
class ProtocolError(BError):
    def __init__(self, t, msg="Protocol Error"):
        super(ProtocolError, self).__init__(t=t, e=[203, msg])
class MethodUnknownError(BError):
    def __init__(self, t, msg="Method Unknow"):
        super(MethodUnknownError, self).__init__(t=t, e=[204, msg])


import dht
