# -*- coding: utf-8 -*-
import utils
from utils import ID 

def check_id(t, id):
    # little hack for some buggy dht client
    if isinstance(id, long):
        id = ('%040x' % id).decode("hex")
    if not isinstance(id, str) and not isinstance(id, ID):
        print "BAD ID %r of type %s" % (id, type(id).__name__)
        raise ProtocolError(t, "id and info_hash should be string")
    elif len(id) != 20:
        print "BAD ID %r of len %s" % (id.encode("hex"), len(id))
        raise ProtocolError(t, "id and info_hash should be 20 byte long")
    return id
def check_str(t, s):
    if not isinstance(s, str):
        raise ProtocolError(t, "String expected")
def check_int(t, i):
    if not isinstance(i, int):
        raise ProtocolError(t, "Integer expected")

class BQuery(object):
    y = "q"
    t = None # string value representing a transaction ID (no more than 16b)
    q = None # string value containing the method name of the query
    a = None # dictionary value containing named arguments to the query
    addr = None
    def __init__(self, t, a, addr):
        self.t = t
        self.a = a
        self.addr = addr
    def __getitem__(self, key):
        return self.a[key]
    def __str__(self):
        return utils.bencode({"y":self.y, "t":self.t, "q":self.q, "a":self.a})
    def __repr__(self):
        return repr({"y":self.y, "t":self.t.encode("hex"), "q":self.q, "a":self.a})

class PingQuery(BQuery):
    q = "ping"
    def __init__(self, t, id, addr=None):
        id = check_id(t, id)
        check_str(t, t)
        super(PingQuery, self).__init__(t, {"id" : ID(id)}, addr)
    def response(self, dht, **kwargs):
        return PingResponse(self.t, dht.myid)

class FindNodeQuery(BQuery):
    q = "find_node"
    def __init__(self, t, id, target, addr=None):
        check_str(t, t)
        id = check_id(t, id)
        target = check_id(t, target)
        super(FindNodeQuery, self).__init__(t, {"id" : ID(id), "target" : ID(target)}, addr)
    def response(self, dht, **kwargs):
        return FindNodeResponse(self.t, dht.myid, dht.get_closest_nodes(self.a["target"]))

class GetPeersQuery(BQuery):
    q = "get_peers"
    def __init__(self, t, id, info_hash, addr=None):
        check_str(t, t)
        id = check_id(t, id)
        info_hash = check_id(t, info_hash)
        super(GetPeersQuery, self).__init__(t, {"id" : ID(id), "info_hash" : ID(info_hash)}, addr)
    def response(self, dht, **kwargs):
        token = dht._get_token(self.addr[0])
        values = dht._get_peers(self.a["info_hash"])
        if values:
            return GetPeersResponse(self.t, dht.myid, token, values=values)
        else:
            nodes = dht.get_closest_nodes(self.a["info_hash"])
            return GetPeersResponse(self.t, dht.myid, token, nodes=nodes)

class AnnouncePeerQuery(BQuery):
    q = "announce_peer"
    def __init__(self, t, id, info_hash, port, token, implied_port=None, addr=None):
        check_str(t, t)
        #check_str(t, token)
        check_str(t, t)
        check_int(t, port)
        id = check_id(t, id)
        info_hash = check_id(t, info_hash)
        # If implied_port is not None and non-zero, the port argument should be ignored and the source port of the UDP packet should be used as the peer's port instead.
        if implied_port is not None:
            super(AnnouncePeerQuery, self).__init__(t, {"id" : ID(id), "info_hash" : ID(info_hash), "port" : port, "token" : ID(token), "implied_port" : implied_port}, addr)
        else:
            super(AnnouncePeerQuery, self).__init__(t, {"id" : ID(id), "info_hash" : ID(info_hash), "port" : port, "token" : ID(token)}, addr)
    def response(self, dht, **kwargs):
        if not self.a["token"] in dht._get_valid_token(self.addr[0]):
            raise ProtocolError("Bad token")
        return AnnouncePeerResponse(self.t, dht.myid)

class BResponse(object):
    y = "r"
    t = None # string value representing a transaction ID
    r = None # dictionary containing named return values
    addr = None
    def __init__(self, t, r, addr):
        self.t = t
        self.r = r
        self.addr = addr
    def __getitem__(self, key):
        return self.r[key]
    def __str__(self):
        return utils.bencode({"y":self.y, "t":self.t, "r":self.r})
    def __repr__(self):
        return repr({"y":self.y, "t":self.t.encode("hex"), "r":self.r})

class PingResponse(BResponse):
    q = "ping"
    def __init__(self, t, id, addr=None):
        check_str(t, t)
        id = check_id(t, id)
        super(PingResponse, self).__init__(t, {"id" : id}, addr)

class FindNodeResponse(BResponse):
    q = "find_node"
    def __init__(self, t, id, nodes, addr=None):
        check_str(t, t)
        id = check_id(t, id)
        if not isinstance(nodes, list):
            raise ProtocolError(t, "nodes should be a list")
        super(FindNodeResponse, self).__init__(t, {"id" : ID(id), "nodes" : nodes}, addr)
    def __str__(self):
        return utils.bencode({"y":self.y, "t":self.t, "r":{"id" : self.r["id"], "nodes" : "".join((n.compact_info() for n in self.r["nodes"]))}})

class GetPeersResponse(BResponse):
    q = "get_peers"
    def __init__(self, t, id, token, values=None, nodes=None, addr=None):
        check_str(t, t)
        #check_str(t, token)
        id = check_id(t, id)
        if nodes is not None:
            if not isinstance(nodes, list):
                raise ProtocolError(t, "nodes should be a list")
            super(GetPeersResponse, self).__init__(t, {"id" : ID(id), "token": ID(token), "nodes":nodes}, addr)
        elif values is not None:
            if not isinstance(values, list):
                raise ProtocolError(t, "values should be a list")
            for ipport in values:
                if not isinstance(ipport, str) or len(ipport) != 6:
                    raise ProtocolError(t, "values elements sould be strings of 6 bytes")
            super(GetPeersResponse, self).__init__(t, {"id" : ID(id), "token": ID(token), "values":values}, addr)
        else:
            raise ValueError("values or nodes needed")
    def __str__(self):
        if "nodes" in self.r:
            return utils.bencode({"y":self.y, "t":self.t, "r":{"id" : self.r["id"], "token" : self.r["token"], "nodes" : "".join((n.compact_info() for n in self.r["nodes"]))}})
        else:
            return super(GetPeersResponse, self).__str__()

class AnnouncePeerResponse(BResponse):
    q = "announce_peer"
    def __init__(self, t, id, addr=None):
        check_str(t, t)
        id = check_id(t, id)
        super(AnnouncePeerResponse, self).__init__(t, {"id" : ID(id)}, addr)

class BError(Exception):
    y = "e"
    t = None # string value representing a transaction ID
    e = None # a list. The first element is an integer representing the error code. The second element is a string containing the error message
    def __init__(self, t, e, **kwargs):
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

