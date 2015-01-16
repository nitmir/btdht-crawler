
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

from libc.stdio cimport printf
from libc.string cimport strlen, strncmp, strcmp, strncpy, strcpy
from libc.stdlib cimport atoi, malloc, free


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

cdef varray_to_list(char ** data, size):
    l=[]
    for i in range(size):
        l.append(data[i][:6])
    return l

cdef char** vlist_to_array(l):
    cdef char ** data = <char**>malloc(len(l) * sizeof(char*))
    for i in range(len(l)):
        data[i]=l[i]
    return data
    
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

    property y:
        def __get__(self):
            if self.has_y:
                return self._y[:self.y_len]
            else:
                return None
        def __set__(self,char* value):
            self.y_len = len(value)
            with nogil:
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
        cdef int i
        cdef char * j
        cdef int l
        if key == b"implied_port" or key == b"port":
            i = value
        else:
            j = value
            l = len(value)
            if key == b"id" or key == b"target" or key == b"info_hash":
                if l != 20:
                    raise ValueError("Can only set strings of length 20B")   
        with nogil:
            if strcmp(key, "id") == 0:
                if self.has_id:
                    free(self.id)
                else:
                    self.has_id = True
                self.id = <char *>malloc(20 * sizeof(char))
                strncpy(self.id, j, 20)
                return
            elif strcmp(key, "target") == 0:
                if self.has_target:
                    free(self.target)
                else:
                    self.has_target = True
                self.target = <char *>malloc(20 * sizeof(char))
                strncpy(self.target, j, 20)
                return
            elif strcmp(key, "info_hash") == 0:
                if self.has_info_hash:
                    free(self.info_hash)
                else:
                    self.has_info_hash = True
                self.info_hash = <char *>malloc(20 * sizeof(char))
                strncpy(self.info_hash, j, 20)
                return
            elif strcmp(key, "token") == 0:
                if self.has_token:
                    free(self.token)
                else:
                    self.has_token = True
                self.token_len = l
                self.token = <char *>malloc(l * sizeof(char))
                strncpy(self.token, j, l)
                return
            elif strcmp(key, "nodes") == 0:
                if self.has_target:
                    free(self.nodes)
                else:
                    self.has_nodes = True
                self.nodes_len = l
                self.nodes = <char *>malloc(l * sizeof(char))
                strncpy(self.nodes, j, l)
                return
            elif strcmp(key, "implied_port") == 0:
                self.implied_port = i
                self.has_implied_port = True
                return
            elif strcmp(key, "port") == 0:
                self.port = i
                self.has_port = True
                return
        raise KeyError(key)
    def get(self, char* key, default=None):
        try:
            self[key]
        except KeyError:
            return default

    def __dealloc__(self):
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
            free(self.values)

    cdef int _decode_string(self, char* data, int* i, int max, int* j) nogil:
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
                    return False
            else:
                return False
        else:
            return False

    cdef int _decode_int(self, char* data, int *i, int max, int *myint) nogil:
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
                
        
    cdef int _decode_dict_elm(self, char* data, int* i, int max) nogil:
        cdef int j[1]
        j[0]=0
        if not self._decode_string(data, i, max, j):
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
            if self._decode_string(data, i, max, j):
                self.t_len = i[0]-j[0]
                self._t = <char *>malloc(self.t_len * sizeof(char))
                self.has_t = True
                strncpy(self._t, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "v", i[0]-j[0]) == 0:
            if self._decode_string(data, i, max, j):
                self.v_len = i[0]-j[0]
                self._v = <char *>malloc(self.v_len * sizeof(char))
                self.has_v = True
                strncpy(self._v, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "y", i[0]-j[0]) == 0:
            if self._decode_string(data, i, max, j):
                self.y_len = i[0]-j[0]
                self._y = <char *>malloc(self.y_len * sizeof(char))
                self.has_y = True
                strncpy(self._y, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "q", i[0]-j[0]) == 0:
            if self._decode_string(data, i, max, j):
                self.q_len = i[0]-j[0]
                self._q = <char *>malloc(self.q_len * sizeof(char))
                self.has_q = True
                strncpy(self._q, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "id", i[0]-j[0]) == 0:
            if self._decode_string(data, i, max, j) and (i[0]-j[0]) == 20:
                self.id = <char *>malloc(20 * sizeof(char))
                self.has_id = True
                strncpy(self.id, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "target", i[0]-j[0]) == 0:
            if self._decode_string(data, i, max, j) and (i[0]-j[0]) == 20:
                self.target = <char *>malloc(20 * sizeof(char))
                self.has_target = True
                strncpy(self.target, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "info_hash", i[0]-j[0]) == 0:
            if self._decode_string(data, i, max, j) and (i[0]-j[0]) == 20:
                self.info_hash = <char *>malloc(20 * sizeof(char))
                self.has_info_hash = True
                strncpy(self.info_hash, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "implied_port", i[0]-j[0]) == 0:
            if self._decode_int(data, i, max, j):
                self.implied_port = j[0]
                self.has_implied_port = True
                return True
            else:
                return False
        elif strncmp(data + j[0], "port", i[0]-j[0]) == 0:
            if self._decode_int(data, i, max, j):
                self.port = j[0]
                self.has_port = True
                return True
            else:
                return False
        elif strncmp(data + j[0], "token", i[0]-j[0]) == 0:
            if self._decode_string(data, i, max, j):
                self.token_len = i[0]-j[0]
                self.token = <char *>malloc(self.token_len * sizeof(char))
                self.has_token = True
                strncpy(self.token, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "nodes", i[0]-j[0]) == 0:
            if self._decode_string(data, i, max, j):
                self.nodes_len = i[0]-j[0]
                self.nodes = <char *>malloc(self.nodes_len * sizeof(char))
                self.has_nodes = True
                strncpy(self.nodes, data + j[0], i[0]-j[0])
                return True
            else:
                return False
        elif strncmp(data + j[0], "values", i[0]-j[0]) == 0:
            pass 
        else:
            printf("error %s %d\n", data + j[0], i[0]-j[0])


        return True
    cdef int _decode_dict(self, char* data, int *i, int max) nogil:
        if data[i[0]] == 'd':
            i[0]+=1
            while data[i[0]] != 'e' and i[0] < max:
                if self._decode_dict_elm(data, i, max):
                    pass
                    #i[0]+=1
                else:
                    break
        if data[i[0]] != 'e':
            return False
        else:
            i[0]+=1
            return True
                
    cdef int _decode(self, char* data, int *i, int max) nogil:
        return self._decode_dict(data, i, max)

    def __cinit__(self, char* data=""):
        cdef int i = 0
        cdef bint valid = False
        with nogil:
            if data[0] != "\0":
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
                valid = self._decode(data, &i, strlen(data))
        if data[0] != "\0" and (not valid or not self.has_q or self.has_t):
            raise EnvironmentError("invalid data")

cdef BMessage init_BMessage(char*data):
        cdef BMessage b = BMessage()
        b.decode("coucou")
        return
