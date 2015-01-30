# -*- coding: utf-8 -*-
import time
import hashlib
import socket
import select
import struct
import math
import collections
from threading import Thread

from btdht.utils import bencode, bdecode, _bdecode, BcodeError, ID

class MetaDataDownloaded(Exception):
    pass
class MetaDataToBig(Exception):
    pass

class ToRead(Exception):
    pass

class Client(object):
    _am_choking = {} # socket -> bool
    _am_interested = {} # socket -> bool
    _peer_choking = {} # socket -> bool
    _peer_interested = {} # socket -> bool

    _am_metadata = 1
    _peer_extended = {} # socket -> bool
    _peer_metadata = {} # socket -> int

    _metadata_size = {}# hash -> int
    _metadata_size_qorum = {} # hash -> size -> nb
    _metadata_pieces = {} # hash -> list/array
    _metadata_pieces_received = {} # hash -> int
    _metadata_pieces_nb = {} # hash -> int

    _socket_hash = {} # socket -> hash
    _socket_toread = {} # socket -> str
    _socket_handshake = {} # socket -> bool
    _socket_ipport = {} # socket -> (ip, port)
    _hash_socket = collections.defaultdict(set) # hash -> socket set
    _peers_socket = {} # (ip, port, hash) -> socket
    _fd_to_socket = {} # int -> socket

    meta_data = {} # hash -> bytes


    stoped = True
    threads = []

    def __init__(self, debug=False):
        self.debug = debug
        self.poll = select.poll()

    def stop(self):
        self.stoped = True
        for i in range(60):
            if not [t for t in self.threads if t.is_alive()]:
                break
            time.sleep(1)
        if [t for t in self.threads if t.is_alive()]:
            print "Unable to stop threads"

    def start(self):
        self.stoped = False
        self.id = str(ID())
        t = Thread(target=self._recv_loop)
        t.setName("Client:recv_loop")
        t.daemon = True
        t.start()
        self.threads.append(t)

    def _recv_loop(self):
        while True:
            if self.stoped:
                return
            try:
                events = self.poll.poll(1000)
                #sockets, _, _ = select.select(self._socket_hash.keys(), [], [], 1)
                for fileno, flag in events:
                    s = self._fd_to_socket[fileno]
                    if (flag & (select.POLLIN|select.POLLPRI)):
                        try:
                            self.recv(s)
                        except (ToRead, socket.timeout):
                            pass
                        except socket.error as e:
                            if e.errno not in [11]:
                                self.clean_socket(s)
                        except (MetaDataToBig, BcodeError, ValueError, KeyError) as e:
                            self.clean_socket(s)
                    elif (flag & (select.POLLERR | select.POLLHUP | select.POLLNVAL)):
                        print flag
                        self.clean_socket(s)
            except (socket.error, select.error) as e:
                if e.args[0] != 9:
                    print e
            except KeyError:
                pass

    def init_hash(self, hash):
        mps = self.most_probably_size(hash)
        self._metadata_size[hash] = mps
        self._metadata_pieces_nb[hash] = int(math.ceil(mps/(16.0*1024)))
        self._metadata_pieces[hash] = [None] * self._metadata_pieces_nb[hash]
        self._metadata_pieces_received[hash] = 0
        self._metadata_size_qorum[hash] = {mps:10}

    def clean_socket(self, s, hash=None):
        def rem(d, s):
            try: del d[s]
            except KeyError: pass
        try:
            if hash is None:
                hash = self._socket_hash[s]
            (ip, port) = self._socket_ipport[s]
            rem(self._peers_socket, (ip, port, hash))
        except KeyError:
            pass
        try: self.poll.unregister(s)
        except (KeyError, select.error, socket.error): pass
        try: del self._fd_to_socket[s.fileno()]
        except (KeyError, socket.error): pass
        rem(self._socket_toread, s)
        rem(self._socket_handshake, s)
        rem(self._socket_hash, s)
        rem(self._peer_metadata, s)
        rem(self._peer_extended, s)
        rem(self._am_choking, s)
        rem(self._am_interested, s)
        rem(self._peer_choking, s)
        rem(self._peer_interested, s)
        rem(self._socket_ipport, s)
        try:self._hash_socket[hash].remove(s)
        except KeyError: pass
        try:s.close()
        except: pass

    def clean_hash(self, hash):
        def rem(d, s):
            try: del d[s]
            except KeyError: pass
        for s in list(self._hash_socket[hash]):
            self.clean_socket(s, hash)
        rem(self._metadata_size, hash)
        rem(self._metadata_pieces, hash)
        rem(self._metadata_pieces_received, hash)
        rem(self._metadata_pieces_nb, hash)
        rem(self._hash_socket, hash)
        rem(self._metadata_size_qorum, hash)

    def add(self, ip, port, hash):
        if hash in self.meta_data:
            return True
        if self.stoped:
            return None
        if not (ip, port, hash) in self._peers_socket:
            try:
                s = socket.create_connection((ip, port), timeout=0.5)
            except (socket.timeout, socket.error, ValueError, BcodeError):
                return False

            self._peers_socket[(ip, port, hash)] = s
            self._socket_ipport[s] = (ip, port)
            self._peer_extended[s] = False
            self._peer_metadata[s] = False
            self._peer_interested[s] = False
            self._peer_choking[s] = True
            self._am_interested[s] = False
            self._am_choking[s] = True
            self._socket_toread[s] = ""
            self._socket_handshake[s] = False
            self._hash_socket[hash].add(s)

            if not hash in self._metadata_size_qorum:
                self._metadata_size_qorum[hash] = {}

            self._socket_hash[s] = hash

            self._fd_to_socket[s.fileno()]=s
            try:
                self.poll.register(s, select.POLLIN|select.POLLHUP|select.POLLERR|select.POLLNVAL|select.POLLPRI)
                self.handshake(s)
                s.settimeout(0.0)
            except (socket.timeout, socket.error, ValueError, BcodeError) as e:
                if self.debug:
                    print "handshake fail:%r" % e
                self.clean_socket(s)
                return False
            return True

    def handle_0(self, s, _): # choke
        self._am_choking[s] = True
    def handle_1(self, s, _): # unchoke
        self._am_choking[s] = False
        #print "requesting %s pieces for %s" % (self._metadata_pieces_nb, self.info_hash.encode("hex"))
        try:
            for i in range(self._metadata_pieces_nb[self._socket_hash[s]]):
                if self._metadata_pieces[self._socket_hash[s]][i] is None:
                    self.metadata_request(s, i)
        except IndexError:
            pass

    def handle_2(self, s, _): # interested
        self._peer_interested[s] = True
    def handle_3(self, s, _): # not interested
        self._peer_interested[s] = False
    def handle_4(self, s, _): # have
        pass
    def handle_5(self, s, payload):
        pass

    def most_probably_size(self, hash):
        s=0
        s_nb=0
        for size, nb in self._metadata_size_qorum[hash].items():
            if nb > s_nb or (nb == s_nb and size < s and size > 0):
                s = size
                s_nb = nb
        return s

    def handle_20(self, s, payload): # extension type
        msg_typ = ord(payload[0])
        msg = payload[1:]
        if msg_typ == 0:
            hash = self._socket_hash[s]
            msg = bdecode(msg)
            if 'metadata_size' in msg:
                if msg['metadata_size'] > 8192000 or msg['metadata_size'] < 1: # plus de 8000ko or less thant 1o
                    self.clean_socket(s)
                if not msg['metadata_size'] in self._metadata_size_qorum[hash]:
                     self._metadata_size_qorum[hash][msg['metadata_size']] = 1
                else:
                     self._metadata_size_qorum[hash][msg['metadata_size']]+=1
                self._peer_metadata[s] = msg['m']['ut_metadata']
                if not hash in self._metadata_size:
                    self._metadata_size[hash] = msg['metadata_size']
                    self._metadata_pieces_nb[hash] = int(math.ceil(self._metadata_size[hash]/(16.0*1024)))
                    self._metadata_pieces[hash] = [None] * self._metadata_pieces_nb[hash]
                    self._metadata_pieces_received[hash] = 0
                else:
                    mps = self.most_probably_size(hash)
                    if self._metadata_size[hash] < mps:
                        piece_nb = int(math.ceil(mps/(16.0*1024)))
                        self._metadata_pieces[hash] = self._metadata_pieces[hash] + ([None] * (piece_nb - self._metadata_pieces_nb[hash]))
                        self._metadata_size[hash] = mps
                        self._metadata_pieces_nb[hash] = piece_nb
                    elif self._metadata_size[hash] > mps:
                        self._metadata_size[hash] = mps
                        self._metadata_pieces_nb[hash] = int(math.ceil(mps/(16.0*1024)))
                        self._metadata_pieces[hash] = self._metadata_pieces[hash][0:self._metadata_pieces_nb[hash]]
                        self._metadata_pieces_received[hash] = len([i for i in self._metadata_pieces[hash] if i is not None])

                self.interested(s)
        elif msg_typ == self._am_metadata:
            hash = self._socket_hash[s]
            msg, data = _bdecode(msg)
            if msg['msg_type'] == 0:
                if self._metadata_pieces[hash] and msg['piece'] < self._metadata_pieces_nb[hash]:
                    self.metadata_data(s, msg['piece'])
                else:
                    self.metadata_reject(s, msg['piece'])
            elif msg['msg_type'] == 1:
                try:
                    if msg['piece'] < self._metadata_pieces_nb[hash] and self._metadata_pieces[hash][msg['piece']] is None:
                        self._metadata_pieces[hash][msg['piece']] = data
                        self._metadata_pieces_received[hash] += 1
                        if self._metadata_pieces_received[hash] == self._metadata_pieces_nb[hash]:
                            metadata = "".join(self._metadata_pieces[hash])
                            if hashlib.sha1(metadata).digest() == hash:
                                self.meta_data[hash] = metadata
                                self.clean_hash(hash)
                                if self.debug:
                                    print "metadata complete"
                            else:
                                self.init_hash(hash)
                                if self.debug:
                                    print "bad metadata %s != %s" % (hashlib.sha1(metadata).hexdigest(), hash.encode("hex"))
                except (IndexError, TypeError):
                    pass
            elif msg['msg_type'] == 2:
                pass
        else:
            pass

    def recv(self, s):
        hash = self._socket_hash[s]
        if hash in self.meta_data:
            self.clean_socket(s)
        msgl_old = len(self._socket_toread[s])
        self._socket_toread[s] += s.recv(4096)
        msgl = len(self._socket_toread[s])
        # if read 0B socket closed
        if msgl_old == msgl:
            raise socket.error("recv 0 bytes")
        while self._socket_toread[s]:
            msg = self._socket_toread[s]
            msgl = len(msg)
            # if handshake not received
            if not self._socket_handshake[s]:
                pstrlen = ord(msg[0])
                if len(msg) < 1+pstrlen+8+20+20:
                    raise ToRead()
                self._socket_handshake[s] = True
                self._socket_toread[s] = msg[1+pstrlen+8+20+20:]
                self.handle_handshake(s, msg[:1+pstrlen+8+20+20])
            else:
                if msgl < 4:
                    raise ToRead()
                msg_len = struct.unpack("!i", msg[:4])[0]
                if msgl < msg_len + 4:
                    raise ToRead()
                if msg_len > 0:
                    msg_typ = ord(msg[4])
                    paypload = msg[5:4+msg_len]
                    self._socket_toread[s] = msg[4+msg_len:]
                    if hasattr(self, "handle_%s" % msg_typ):
                        getattr(self, "handle_%s" % msg_typ)(s, paypload)
                else:
                    self._socket_toread[s] = msg[4:]
        


    def recv_fixlen(self, s, i):
        msg = ""
        msgl = 0
        while msgl < i:
            tmp = s.recv(i-msgl)
            if tmp == "":
                raise socket.error("recv 0 bytes")
            msg+=tmp
            msgl+=len(tmp)
        return msg

    def handle_handshake(self, s, msg):
        pstrlen = ord(msg[0])
        pstr = msg[1:1+pstrlen]
        reserved = msg[1+pstrlen:1+pstrlen+8]
        self._peer_extended[s] = (ord(reserved[5]) & 16) == 16
        info_hash = msg[1+pstrlen+8:1+pstrlen+8+20]
        peer_id = msg[1+pstrlen+8+20:1+pstrlen+8+20+20]
        if self._peer_extended[s]:
            self.extended_handshake(s)
        else:
            self.clean_socket(s)
        
    def handshake(self, s):
        reserved_bits = ["\0","\0","\0","\0","\0","\0","\0","\0"]
        # advertise Extension Protocol
        reserved_bits[5]= chr(ord(reserved_bits[5]) | ord('\x10'))
        reserved_bits="".join(reserved_bits)
        msg="%sBitTorrent protocol%s%s%s" % (chr(19), reserved_bits, self._socket_hash[s], self.id)
        #print "%r" % msg
        s.send(msg)
        #pstrlen = ord(self.recv_fixlen(s, 1))
        #pstr = self.recv_fixlen(s, pstrlen)
        #reserved = self.recv_fixlen(s, 8)
        #self._peer_extended[s] = (ord(reserved[5]) & 16) == 16
        #info_hash = self.recv_fixlen(s, 20)
        #peer_id = self.recv_fixlen(s, 20)
        #print "pstr:%s" % pstr
        #print "reserver:{0:064b}".format(int(reserved.encode("hex"), 16))
        #print "info_hash:%r" % info_hash
        

    def keep_alive(self, s):
        msg=struct.pack("!i", 0)
        s.send(msg)

    def choke(self, s):
        msg=struct.pack("!ib", 1, 0)
        s.send(msg)

    def unckoke(self, s):
        msg=struct.pack("!ib", 1, 1)
        s.send(msg)

    def interested(self, s):
        if self._am_interested[s]:
            raise ValueError("already interested")
        msg=struct.pack("!ib", 1, 2)
        s.send(msg)
        self._am_interested[s] = True

    def notinterested(self, s):
        if not self._am_interested[s]:
            raise ValueError("already not interested")
        msg=struct.pack("!ib", 1, 3)
        s.send(msg)
        self._am_interested[s] = False

    def have(self, s, piece_index):
        msg=struct.pack("!ibi", 5, 4, piece_index)
        s.send(msg)

    def extended_handshake(self, s):
        if not self._peer_extended[s]:
            raise ValueError("Peer does not support extension protocol")
        pl = bencode({'m':{'ut_metadata': self._am_metadata}})
        msg=struct.pack("!ibb", 1 + 1 + len(pl), 20, 0)
        msg+=pl
        s.send(msg)
        

    def metadata_request(self, s, piece):
        if not self._peer_metadata[s]:
            raise ValueError("peer does not support metadata extension")
        if self._am_choking[s]:
            raise ValueError("chocked")
        if piece < self._metadata_pieces_nb[self._socket_hash[s]]:
            pl = bencode({'msg_type': 0, 'piece': piece})
            msg=struct.pack("!ibb", 1 + 1 + len(pl), 20, self._peer_metadata[s])
            msg+=pl
            s.send(msg)

    def metadata_reject(self, s, piece):
        if not self._peer_metadata[s]:
            raise ValueError("peer does not support metadata extension")
        if self._am_choking[s]:
            raise ValueError("chocked")
        pl = bencode({'msg_type': 2, 'piece': piece})
        msg=struct.pack("!ibb", 1 + 1 + len(pl), 20, self._peer_metadata[s])
        msg+=pl
        s.send(msg)

    def metadata_data(self, s, piece):
        if not self._peer_metadata[s]:
            raise ValueError("peer does not support metadata extension")
        if self._am_choking[s]:
            raise ValueError("chocked")
        pl = bencode({'msg_type': 1, 'piece': piece, 'total_size': len(self._metadata_pieces[self._socket_hash[s]][piece])})
        pl += self._metadata_pieces[self._socket_hash[s]][piece]
        msg=struct.pack("!ibb", 1 + 1 + len(pl), 20, self._peer_metadata[s])
        msg+=pl
        s.send(msg)

