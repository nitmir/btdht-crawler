
"""
y -> q | r | e
t -> str|int
q -> str
a -> dict
 id -> 20str
 target -> 20str
 info_hash -> 20str
 implied_port -> int (bool 0 ou 1)
 port -> short int
 token -> str|int
v
r
 id -> 20str
 nodes -> str
 values -> list of str
 token -> str|int
"""

from libc cimport math
from libc.stdio cimport printf, sprintf
from libc.string cimport strlen, strncmp, strcmp, strncpy, strcpy
from libc.stdlib cimport atoi, malloc, free
from cython.parallel import prange

import utils

cdef int str_to_int(char* data, int len) nogil:
    cdef char* msg
    cdef int i
    try:
        msg = <char *>malloc((len+1) * sizeof(char))
        strncpy(msg, data, len)
        msg[len]='\0'
        i = atoi(msg)
    finally:
        free(msg)
    return i

cdef int int_length(int i) nogil:
    if i == 0:
        return 1
    elif i < 0:
        return (<int> math.log10(0-i)) + 2
    else:
        return (<int> math.log10(i)) + 1

cdef varray_to_list(char ** data, size):
    l=[]
    for i in range(size):
        l.append(data[i][:6])
    return l

cdef char** vlist_to_array(l, int size=6):
    cdef char ** data = <char**>malloc(len(l) * sizeof(char*))
    for i in range(len(l)):
        if len(l[i]) != size:
            raise ValueError("list element should be of length %d\n" % size)
        data[i]=<char*>malloc(6 * sizeof(char))
        strncpy(data[i], l[i], 6)
    return data



cdef bint _decode_string(char* data, int* i, int max, int* j) nogil:
    cdef bint ret
    if data[i[0]] == '0' \
    or data[i[0]] == '2' \
    or data[i[0]] == '3' \
    or data[i[0]] == '4' \
    or data[i[0]] == '5' \
    or data[i[0]] == '6' \
    or data[i[0]] == '7' \
    or data[i[0]] == '8' \
    or data[i[0]] == '9' \
    or data[i[0]] == '1':
        j[0]=i[0]+1
        while data[j[0]] != ':' and j[0] < max:
            j[0]+=1
        if data[j[0]] == ':':
            i[0] = j[0] + str_to_int(data + i[0], j[0]-i[0]) + 1
            j[0]+=1
            if i[0] <= max:
                return True
            else:
                with gil:
                     raise ValueError("%s > %s : %s" % (i[0], max, data))
        else:
            with gil:
                raise ValueError("%s != : at %s %s" % (data[j[0]], j[0], data))
    else:
        return False

cdef bint _decode_int(char* data, int *i, int max, int *myint) nogil:
    cdef int j
    if data[i[0]] == 'i':
        i[0]+=1
        j = i[0]
        while data[j]!='e' and j < max:
            j+=1
        if data[j] == 'e':
            myint[0]=str_to_int(data + i[0], j-i[0])
            i[0]=j+1
            if i[0] <= max:
                return True
            else:
                return False
        else:
            return False
    else:
        return False
                
cdef bint _encode_int(char* data, int *i, int max, int j) nogil:
    cdef int l
    l = int_length(j)
    if max >= i[0] + l + 2:
         data[i[0]]='i'
         i[0]+=1
         sprintf(data + i[0], "%d", j)
         i[0]+=l
         data[i[0]]='e'
         i[0]+=1
         return True
    else:
        printf("encode_int: %d < %d\n", max, i[0] + l + 2)
        return False

cdef bint _encode_string(char* data, int* i, int max, char* str, int strlen) nogil:
    cdef int l
    l = int_length(strlen)
    if max >= i[0] + l + 1 + strlen: # size as char + : + string
        sprintf(data + i[0], "%d", strlen)
        i[0]+=l
        data[i[0]]=':'
        i[0]+=1
        strncpy(data + i[0], str, strlen)
        i[0]+=strlen
        return True
    else:
        printf("encode_string: %d < %d\n", max, i[0] + l + 1 + strlen)
        return False

class BError(Exception):
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


cdef class BMessage:
    cdef char* _y
    cdef bint has_y
    cdef int y_len
    cdef char* _t
    cdef bint has_t
    cdef int t_len
    cdef char* _q
    cdef bint has_q
    cdef int q_len
    cdef char* _v
    cdef bint has_v
    cdef int v_len
    cdef bint r
    cdef bint a
    cdef char* id
    cdef bint has_id
    cdef char* target
    cdef bint has_target
    cdef char* info_hash
    cdef bint has_info_hash
    cdef int implied_port
    cdef bint has_implied_port
    cdef int port
    cdef bint has_port
    cdef char* token
    cdef bint has_token
    cdef int token_len
    cdef char* nodes
    cdef bint has_nodes
    cdef int nodes_len
    cdef char** values
    cdef int values_nb
    cdef bint has_values
    cdef char* encoded
    cdef int encoded_len
    cdef bint encoded_uptodate
    cdef bint debug
    cdef char* addr_addr
    cdef int addr_port

    cdef bint set_y(self, char* value, int size) nogil:
        self.encoded_uptodate = False
        if self.has_y:
            free(self._y)
        else:
            self.has_y = True
        self._y = <char*>malloc(size * sizeof(char))
        self.y_len = size
        strncpy(self._y, value, size)
        return True

    cdef bint set_id(self, char* value, int size) nogil:
        self.encoded_uptodate = False
        if size != 20:
            return False
        if self.has_id:
            free(self.id)
        else:
            self.has_id = True
        self.id = <char*>malloc(size * sizeof(char))
        strncpy(self.id, value, size)
        return True

    cdef bint set_t(self, char* value, int size) nogil:
        self.encoded_uptodate = False
        if self.has_t:
            free(self._t)
        else:
            self.has_t = True
        self._t = <char*>malloc(size * sizeof(char))
        self.t_len = size
        strncpy(self._t, value, size)
        return True

    cdef bint set_r(self, bint value) nogil:
        self.encoded_uptodate = False
        self.r = value
        return True

    cdef bint set_nodes(self, char* value, int size) nogil:
        self.encoded_uptodate = False
        if self.has_nodes:
            free(self.nodes)
        else:
            self.has_nodes = True
        self.nodes_len = size
        self.nodes = <char*>malloc(size * sizeof(char))
        strncpy(self.nodes, value, size)
        return True

    cdef bint set_values(self, char** values) nogil:
        cdef int i
        self.encoded_uptodate = False
        if self.has_values:
            for i in prange(self.values_nb):
                free(self.values[i])
            free(self.values)
        else:
            self.has_values = True
        self.values = values

    cdef bint set_token(self, char* value, int size) nogil:
        self.encoded_uptodate = False
        if self.has_token:
            free(self.token)
        else:
            self.has_token = True
        self.token_len = size
        self.token = <char*>malloc(size * sizeof(char))
        strncpy(self.token, value, size)
        return True

    def response(self, dht):
        cdef BMessage rep = BMessage()
        cdef char* id = NULL
        cdef int l1 = 0
        cdef int l2 = 0
        cdef char* nodes = NULL
        cdef char* token = NULL
        cdef char** values = NULL
        s = str(dht.myid)
        id = s
        with nogil:
            if self.has_y and self.y_len == 1 and strncmp(self._y, "q", 1) == 0:
                if self.has_q:
                    if self.q_len == 4 and strncmp(self._q, "ping", 4) == 0:
                        rep.set_y("r", 1)
                        rep.set_t(self._t, self.t_len)
                        rep.set_r(True)
                        rep.set_id(id, 20)
                        self._encode()
                        with gil:
                            return rep
                    elif self.q_len == 9 and strncmp(self._q, "find_nodes", 9) == 0:
                        if not self.has_target:
                            with gil:
                                raise ProtocolError(self.t, "target missing")
                        rep.set_y("r", 1)
                        rep.set_t(self._t, self.t_len)
                        rep.set_r(True)
                        rep.set_id(id, 20)
                        with gil:
                            s = dht.get_closest_nodes(self.target[:20], compact=True)
                            nodes = s
                            l1 = len(nodes)
                        rep.set_nodes(nodes, l1)
                        self._encode()
                        with gil:
                            return rep
                    elif self.q_len == 9 and strncmp(self._q, "get_peers", 9) == 0:
                        if not self.has_info_hash:
                            with gil:
                                raise ProtocolError(self.t, "info_hash missing")
                        rep.set_y("r", 1)
                        rep.set_t(self._t, self.t_len)
                        rep.set_r(True)
                        rep.set_id(id, 20)
                        with gil:
                            s = dht._get_token(self.addr[0])
                            token = s
                            l1 = len(s)
                            s = dht._get_peers(self.info_hash[:20])
                            if s:
                                values = vlist_to_array(s)
                            else:
                                s = dht.get_closest_nodes(self.target[:20], compact=True)
                                nodes = s
                                l2 = len(nodes)
                        rep.set_token(token, l1)
                        if values != NULL:
                            rep.set_values(values)
                        else:
                            rep.set_nodes(nodes, l2)
                        self._encode()
                        with gil:
                            return rep
                    elif self.q_len == 13 and strncmp(self._q, "announce_peer", 13) == 0:
                        if not self.has_info_hash:
                            with gil:
                                raise ProtocolError(self.t, "info_hash missing")
                        if not self.has_port:
                            with gil:
                                raise ProtocolError(self.t, "port missing")
                        if not self.has_token:
                            with gil:
                                raise ProtocolError(self.t, "token missing")
                        with gil:
                            s = dht._get_token(self.addr[0])
                            if not self["token"] in s:
                                raise ProtocolError(self.t, "bad token")
                        rep.set_y("r", 1)
                        rep.set_t(self._t, self.t_len)
                        rep.set_r(True)
                        rep.set_id(id, 20)
                        self._encode()
                        with gil:
                            return
                    else:
                        with gil:
                            raise MethodUnknownError(self.t, "Method %s Unknown" % self.q)
                else:
                    printf("not ping %d\n", 0)
            else:
                printf("not query %d\n", 1)

    cdef bint _encode_values(self, char* data, int* i, int max) nogil:
        cdef int j
        if i[0] + self.values_nb * 8 + 2 > max:
            printf("encode_values: %d < %d\n", max, i[0] + self.values_nb * 8 + 2)
            return False
        data[i[0]]='l'
        i[0]+=1
        for j in prange(self.values_nb, nogil=True):
            #printf("encode value %d in encode_values\n", j)
            strncpy(data + i[0],"6:", 2)
            i[0]+=2
            strncpy(data + i[0], self.values[j], 6)
            i[0]+=6
        data[i[0]]='e'
        i[0]+=1
        return True
        
    cdef bint _encode_secondary_dict(self, char* data, int* i, int max) nogil:
        if i[0] + 1 > max:
            printf("encode_secondary:%d\n", 0)
            return False
        data[i[0]] = 'd'
        i[0]+=1
        if self.has_id:
            if i[0] + 4 > max:
                printf("encode_secondary:%d\n", 1)
                return False
            strncpy(data + i[0], "2:id", 4)
            i[0]+=4
            if not _encode_string(data, i, max, self.id, 20):
                return False
        if self.has_implied_port:
            if i[0] + 15 > max:
                printf("encode_secondary:%d\n", 2)
                return False
            strncpy(data + i[0], "12:implied_port", 15)
            i[0]+=15
            if not _encode_int(data, i, max, self.implied_port):
                return False
        if self.has_info_hash:
            if i[0] + 11 > max:
                printf("encode_secondary:%d\n", 3)
                return False
            strncpy(data + i[0], "9:info_hash", 11)
            i[0]+=11
            if not _encode_string(data, i, max, self.info_hash, 20):
                return False
        if self.has_nodes:
            if i[0] + 7 > max:
                printf("encode_secondary:%d\n", 4)
                return False
            strncpy(data + i[0], "5:nodes", 7)
            i[0]+=7
            if not _encode_string(data, i, max, self.nodes, self.nodes_len):
                return False
        if self.has_port:
            if i[0] + 6 > max:
                printf("encode_secondary:%d\n", 5)
                return False
            strncpy(data + i[0], "4:port", 6)
            i[0]+=6
            if not _encode_int(data, i, max, self.port):
                return False
        if self.has_target:
            if i[0] + 8 > max:
                printf("encode_secondary:%d\n", 6)
                return False
            strncpy(data + i[0], "6:target", 8)
            i[0]+=8
            if not _encode_string(data, i, max, self.target, 20):
                return False
        if self.has_token:
            if i[0] + 7 > max:
                printf("encode_secondary:%d\n", 7)
                return False
            strncpy(data + i[0], "5:token", 7)
            i[0]+=7
            if not _encode_string(data, i, max, self.token, self.token_len):
                return False
        if self.has_values:
            if i[0] + 8 > max:
                printf("encode_secondary:%d\n", 8)
                return False
            strncpy(data + i[0], "6:values", 8)
            i[0]+=8
            if not self._encode_values(data, i, max):
                return False
        if i[0] + 1 > max:
            printf("encode_secondary:%d\n", 9)
            return False
        data[i[0]] = 'e'
        i[0]+=1
        return True

    cdef bint _encode_main_dict(self, char* data, int* i, int max) nogil:
        if i[0] + 1 > max:
            printf("encode_main: %d\n", 0)
            return False
        data[i[0]] = 'd'
        i[0]+=1
        if self.a:
            if i[0] + 3 > max:
                printf("encode_main: %d\n", 1)
                return False
            strncpy(data + i[0], "1:a", 3)
            i[0]+=3
            if not self._encode_secondary_dict(data, i, max):
                return False
        if self.has_q:
            if i[0] + 3 > max:
                printf("encode_main: %d\n", 2)
                return False
            strncpy(data + i[0], "1:q", 3)
            i[0]+=3
            if not _encode_string(data, i, max, self._q, self.q_len):
                return False
        if self.r:
            if i[0] + 3 > max:
                printf("encode_main: %d\n", 3)
                return False
            strncpy(data + i[0], "1:r", 3)
            i[0]+=3
            if not self._encode_secondary_dict(data, i, max):
                return False
        if self.has_t:
            if i[0] + 3 > max:
                printf("encode_main: %d\n", 4)
                return False
            strncpy(data + i[0], "1:t", 3)
            i[0]+=3
            if not _encode_string(data, i, max, self._t, self.t_len):
                return False
        if self.has_v:
            if i[0] + 3 > max:
                printf("encode_main: %d\n", 5)
                return False
            strncpy(data + i[0], "1:v", 3)
            i[0]+=3
            if not _encode_string(data, i, max, self._v, self.v_len):
                return False
        if self.has_y:
            if i[0] + 3 > max:
                printf("encode_main: %d\n", 6)
                return False
            strncpy(data + i[0], "1:y", 3)
            i[0]+=3
            if not _encode_string(data, i, max, self._y, self.y_len):
                return False
        if i[0] + 1 > max:
            printf("encode_main: %d\n", 7)
            return False
        data[i[0]] = 'e'
        i[0]+=1
        return True


    cdef bint _encode(self) nogil:
        cdef int i=0
        self.encoded_len = self._encode_len()
        #printf("free%d\n", 0)
        free(self.encoded)
        #printf("free%d\n", 1)
        self.encoded = <char *> malloc(self.encoded_len * sizeof(char))
        if self._encode_main_dict(self.encoded, &i, self.encoded_len):
            self.encoded_uptodate = True
            return True
        else:
            self.encoded_uptodate = False
            return False

    cdef int _encode_len(self) nogil:
        cdef int estimated_len = 2 # the d and e of the global dict
        if self.has_y:
            estimated_len+=int_length(self.y_len) + 1 + self.y_len + 3# len + : + str
        if self.has_t:
            estimated_len+=int_length(self.t_len) + 1 + self.t_len + 3
        if self.has_q:
            estimated_len+=int_length(self.q_len) + 1 + self.q_len + 3
        if self.has_v:
            estimated_len+=int_length(self.v_len) + 1 + self.v_len + 3
        if self.r or self.a: # only one can be True
            estimated_len+=2 + 3 # the d and e of the a ou r dict
        if self.has_id:
            estimated_len+=23 + 4
        if self.has_target:
            estimated_len+=23 + 8
        if self.has_info_hash:
            estimated_len+=23 + 11
        if self.has_implied_port:
            estimated_len+=int_length(self.implied_port) + 1 + 2 + 15# i + int + e
        if self.has_port:
            estimated_len+=int_length(self.port) + 1 + 2 + 6
        if self.has_nodes:
            estimated_len+=int_length(self.nodes_len) + 1 + 1 + self.nodes_len + 7
        if self.has_token:
            estimated_len+=int_length(self.token_len) + 1 + 1 + self.token_len + 7
        if self.has_values:
            estimated_len+= 8 * self.values_nb + 2 + 8 # l + nb * IPPORT + e
        #printf("estimated_len: %d\n" , estimated_len)
        return estimated_len
        
    def encode(self):
        if self.encoded_uptodate or self._encode():
            return self.encoded[:self.encoded_len]
        else:
            raise EnvironmentError("Unable to encode BMessage")

    def __str__(self):
        return self.encode()

    property addr:
        def __get__(self):
            if self.addr_addr and self.addr_port:
                return (self.addr_addr, self.addr_port)
            else:
                return None
        def __set__(self, addr):
            if addr is not None:
                self.addr_addr = addr[0]
                self.addr_port = addr[1]
    property y:
        def __get__(self):
            if self.has_y:
                return self._y[:self.y_len]
            else:
                return None
        def __set__(self,char* value):
            self.y_len = len(value)
            with nogil:
                self.encoded_uptodate = False
                if self.has_y:
                    free(self._y)
                else:
                    self.has_y = True
                self._y = <char *>malloc(self.y_len * sizeof(char))
                strncpy(self._y, value, self.y_len)

    property t:
        def __get__(self):
            if self.has_t:
                return self._t[: self.t_len]
            else:
                return None
        def __set__(self,char* value):
            self.t_len = len(value)
            with nogil:
                self.encoded_uptodate = False
                if self.has_t:
                    free(self._t)
                else:
                    self.has_t = True
                self._t = <char *>malloc(self.t_len * sizeof(char))
                strncpy(self._t, value, self.t_len)
    property q:
        def __get__(self):
            if self.has_q:
                return self._q[: self.q_len]
            else:
                return None
        def __set__(self,char* value):
            self.q_len = len(value)
            with nogil:
                self.encoded_uptodate = False
                if self.has_q:
                    free(self._q)
                else:
                    self.has_q = True
                self._q = <char *>malloc(self.q_len * sizeof(char))
                strncpy(self._q, value, self.q_len)
    property v:
        def __get__(self):
            if self.has_v:
                return self._v[: self.v_len]
            else:
                return None
        def __set__(self,char* value):
            self.v_len = len(value)
            with nogil:
                self.encoded_uptodate = False
                if self.has_v:
                    free(self._v)
                else:
                    self.has_v = True
                self._v = <char *>malloc(self.v_len * sizeof(char))
                strncpy(self._v, value, self.v_len)

    def __getitem__(self, char* key):
        cdef char* msg
        cdef int i
        cdef int typ=-1
        if key == b"id" and self.has_id:
            return self.id[:20]
        elif key == b"target" and self.has_target:
            return self.target[:20]
        elif key == b"info_hash" and self.has_info_hash:
            return self.info_hash[:20]
        elif key == b"token" and self.has_token:
            return self.token[:self.token_len]
        elif key == b"nodes" and self.has_nodes:
            return self.nodes[:self.nodes_len]
        elif key == b"implied_port" and self.has_implied_port:
            return self.implied_port
        elif key == b"port" and self.has_port:
            return self.port
        elif key == b"values" and self.has_values:
            return varray_to_list(self.values, self.values_nb)
        else:
            raise KeyError(key)

    def __setitem__(self, char* key, value):
        cdef int i = 0
        cdef char * j
        cdef char** v
        cdef int l = 0
        with nogil:
            if strcmp(key, "id") == 0:
                with gil:
                    if len(value) != 20:
                        raise ValueError("Can only set strings of length 20B")
                    j = value
                self.set_id(j, 20)
                return
            elif strcmp(key, "target") == 0:
                self.encoded_uptodate = False
                with gil:
                    if len(value) != 20:
                        raise ValueError("Can only set strings of length 20B")
                    j = value
                if self.has_target:
                    free(self.target)
                else:
                    self.has_target = True
                self.target = <char *>malloc(20 * sizeof(char))
                strncpy(self.target, j, 20)
                return
            elif strcmp(key, "info_hash") == 0:
                with gil:
                    if len(value) != 20:
                        raise ValueError("Can only set strings of length 20B")
                    j = value
                self.encoded_uptodate = False
                if self.has_info_hash:
                    free(self.info_hash)
                else:
                    self.has_info_hash = True
                self.info_hash = <char *>malloc(20 * sizeof(char))
                strncpy(self.info_hash, j, 20)
                return
            elif strcmp(key, "token") == 0:
                with gil:
                    l = len(value)
                    j = value
                self.set_token(j, l)
                return
            elif strcmp(key, "nodes") == 0:
                with gil:
                    l = len(value)
                    j = value
                self.set_nodes(j, l)
                return
            elif strcmp(key, "implied_port") == 0:
                with gil:
                    i = value
                self.encoded_uptodate = False
                self.implied_port = i
                self.has_implied_port = True
                return
            elif strcmp(key, "port") == 0:
                with gil:
                    i = value
                self.encoded_uptodate = False
                self.port = i
                self.has_port = True
                return
            elif strcmp(key, "values") == 0:
                with gil:
                    v = vlist_to_array(value)
                    self.values_nb = len(value)
                self.set_values(v)
                return
        raise KeyError(key)

    def get(self, char* key, default=None):
        try:
            return self[key]
        except KeyError as e:
            return default

    def __dealloc__(self):
        cdef int i
        with nogil:
            free(self._y)
            free(self._t)
            free(self._q)
            free(self._v)
            free(self.id)
            free(self.target)
            free(self.info_hash)
            free(self.token)
            free(self.nodes)
            free(self.encoded)
            for i in prange(self.values_nb):
                free(self.values[i])
            free(self.values)

        
    cdef bint _decode_dict_elm(self, char* data, int* i, int max) nogil:
        cdef int j[1]
        j[0]=0
        if not _decode_string(data, i, max, j):
            return False
        
        if strncmp(data + j[0], "a", i[0]-j[0]) == 0:
            if self._decode_dict(data, i, max):
                self.a = True
                return True
            else:
                return False
        elif strncmp(data + j[0], "r", i[0]-j[0]) == 0:
            if self._decode_dict(data, i, max):
                self.r = True
                return True
            else:
                return False
        elif strncmp(data + j[0], "t", i[0]-j[0]) == 0:
            if _decode_string(data, i, max, j):
                self.t_len = i[0]-j[0]
                self._t = <char *>malloc(self.t_len * sizeof(char))
                self.has_t = True
                strncpy(self._t, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "v", i[0]-j[0]) == 0:
            if _decode_string(data, i, max, j):
                self.v_len = i[0]-j[0]
                self._v = <char *>malloc(self.v_len * sizeof(char))
                self.has_v = True
                strncpy(self._v, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "y", i[0]-j[0]) == 0:
            if _decode_string(data, i, max, j):
                self.y_len = i[0]-j[0]
                self._y = <char *>malloc(self.y_len * sizeof(char))
                self.has_y = True
                strncpy(self._y, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "q", i[0]-j[0]) == 0:
            if _decode_string(data, i, max, j):
                self.q_len = i[0]-j[0]
                self._q = <char *>malloc(self.q_len * sizeof(char))
                self.has_q = True
                strncpy(self._q, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "id", i[0]-j[0]) == 0:
            if _decode_string(data, i, max, j) and (i[0]-j[0]) == 20:
                self.id = <char *>malloc(20 * sizeof(char))
                self.has_id = True
                strncpy(self.id, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "target", i[0]-j[0]) == 0:
            if _decode_string(data, i, max, j) and (i[0]-j[0]) == 20:
                self.target = <char *>malloc(20 * sizeof(char))
                self.has_target = True
                strncpy(self.target, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "info_hash", i[0]-j[0]) == 0:
            if _decode_string(data, i, max, j) and (i[0]-j[0]) == 20:
                self.info_hash = <char *>malloc(20 * sizeof(char))
                self.has_info_hash = True
                strncpy(self.info_hash, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "implied_port", i[0]-j[0]) == 0:
            if _decode_int(data, i, max, j):
                self.implied_port = j[0]
                self.has_implied_port = True
                return True
            else:
                return False
        elif strncmp(data + j[0], "port", i[0]-j[0]) == 0:
            if _decode_int(data, i, max, j):
                self.port = j[0]
                self.has_port = True
                return True
            else:
                return False
        elif strncmp(data + j[0], "token", i[0]-j[0]) == 0:
            if _decode_string(data, i, max, j):
                self.token_len = i[0]-j[0]
                self.token = <char *>malloc(self.token_len * sizeof(char))
                self.has_token = True
                strncpy(self.token, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "nodes", i[0]-j[0]) == 0:
            if _decode_string(data, i, max, j):
                self.nodes_len = i[0]-j[0]
                self.nodes = <char *>malloc(self.nodes_len * sizeof(char))
                self.has_nodes = True
                strncpy(self.nodes, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "values", i[0]-j[0]) == 0:
            if self._decode_values(data, i, max):
                return True
            else:
                return False
        else:
            printf("error %s %d\n", data + j[0], i[0]-j[0])


        return True

    cdef bint _decode_values(self, char* data, int *i, int max) nogil:
        cdef int j[1]
        cdef int c = 0
        cdef int k = i[0] + 1
        if not data[i[0]] == 'l':
            return False
        i[0]+=1
        while _decode_string(data, i, max, j):
            if (i[0]-j[0]) != 6:
                if self.debug:
                    with gil:
                        raise ValueError("element of values are expected to be of length 6 and not %s" % (i[0]-j[0]))
                return False
            c+=1
        if i[0] >=  max or data[i[0]] != 'e':
            if self.debug:
                with gil:
                    raise ValueError("End of values list not found %s >= %s found %s elements" % (i[0], max, c))
            return False
        self.values_nb = c
        self.values = <char **>malloc(self.values_nb * sizeof(char*))
        i[0] = k
        c=0
        while _decode_string(data, i, max, j):
           self.values[c] = <char *>malloc( 6 * sizeof(char))
           strncpy(self.values[c], data + j[0], 6)
           c+=1
        self.has_values = True
        i[0]+=1
        return True
            
    cdef bint _decode_dict(self, char* data, int *i, int max) nogil:
        if data[i[0]] == 'd':
            i[0]+=1
            while data[i[0]] != 'e' and i[0] < max:
                if self._decode_dict_elm(data, i, max):
                    pass
                    #i[0]+=1
                else:
                    break
        if data[i[0]] != 'e':
            if self.debug:
                with gil:
                    raise ValueError("End of dict not found %s %s" % (i[0], data))
            return False
        else:
            i[0]+=1
            return True
                
    cdef bint _decode(self, char* data, int *i, int max) nogil:
        return self._decode_dict(data, i, max)

    def  __init__(self, data="", addr=None):
        self.addr = addr

    def __cinit__(self, char* data="", addr=None):
        cdef int i = 0
        cdef bint valid = False
        with nogil:
            self.values_nb = 0
            self.r = False
            self.a = False
            self.has_y = False
            self.has_t = False
            self.has_q = False
            self.has_v = False
            self.has_id = False
            self.has_target = False
            self.has_info_hash = False
            self.has_token = False
            self.has_nodes = False
            self.has_values = False
            self.encoded_uptodate = False
            self.debug = False
            
            if data[0] != "\0":
                valid = self._decode(data, &i, strlen(data))
                if valid:
                    self.encoded_len = self._encode_len()
                    self.encoded = <char *> malloc(self.encoded_len * sizeof(char))
                    strncpy(self.encoded, data, self.encoded_len)
                    self.encoded_uptodate = True
                if not valid or not self.has_t or not self.has_y:
                    with gil:
                        if self.has_t:
                            raise ProtocolError(self._t[:self.t_len])
                        else:
                            raise ProtocolError("")
            
