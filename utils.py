# -*- coding: utf-8 -*-
from functools import total_ordering
def nbit(s, n):
    """Renvois la valeur du nième bit de la chaine s"""
    c=s[n/8]
    return int(format(ord(c), '08b')[n % 8])

def nflip(s, n):
    """Renvois la chaine s dont la valeur du nième bit a été retourné"""
    bit = [0b10000000, 0b01000000, 0b00100000, 0b00010000, 0b00001000, 0b00000100, 0b00000010, 0b00000001]
    return s[:n/8]  + chr(ord(s[n/8]) ^ bit[n % 8]) + s[n/8+1:]

class BcodeError(Exception):
    pass

def random(size):
    with open("/dev/urandom") as f:
        return f.read(size)

@total_ordering
class ID(object):
    def __generate(self):
        return random(20)

    def __init__(self, id=None):
        if id is None:
            self.value = self.__generate()
        else:
            self.value = id

    def startswith(self, s):
        return self.value.startswith(s)

    def __getitem__(self, i):
        return self.value[i]

    def __str__(self):
        return self.value

    def __repr__(self):
        return repr(self.value)

    def __eq__(self, other):    
        if isinstance(other, ID):
            return self.value == other.value
        elif isinstance(other, str):
            return self.value == other
        else:
            return false

    def __lt__(self, other):
        if isinstance(other, ID):
            return self.value < other.value
        elif isinstance(other, str):
            return self.value < other
        else:
            raise TypeError("unsupported operand type(s) for <: 'ID' and '%s'" % type(other).__name__)
            
    def __len__(self):
        return len(self.value)

    def __xor__(self, other):
        if isinstance(other, ID):
            return ''.join(chr(ord(a) ^ ord(b)) for a,b in zip(self.value, other.value))
        elif isinstance(other, str):
            return ''.join(chr(ord(a) ^ ord(b)) for a,b in zip(self.value, other))
        else:
            raise TypeError("unsupported operand type(s) for ^: 'ID' and '%s'" % type(other).__name__)

    def __rxor__(self, other):
        return self.__xor__(other)

    def __hash__(self):
        return hash(self.value)

def bencode(obj):
    try:
        return _bencode(obj)
    except:
        print("%r" % obj)
        raise
def _bencode(obj):
    if isinstance(obj, int):
        return b"i" + str(obj).encode() +  b"e"
    elif isinstance(obj, bytes):
        return str(len(obj)).encode() + b":" + obj
    elif isinstance(obj, str) or isinstance(obj, ID):
        return str(len(obj)).encode() + b":" + str(obj).encode()
    elif isinstance(obj, list):
        return b"l" + b"".join(_bencode(o) for o in obj) + b"e"
    elif isinstance(obj, dict):
        l = list(obj.items())
        l.sort()
        d = []
        for (k, v) in l:
            d.append(k)
            d.append(v)
        return b"d" + b"".join(_bencode(o) for o in d) + b"e"
    else:
        raise EnvironmentError("Can only encode int, str, list or dict, not %s" % type(obj).__name__)

def bdecode(s):
    return _bdecode(s)[0]

def _bdecode(s):
    if not s:
        raise BcodeError("Empty bcode")
    if s[0:1] == b"i":
        try:
            i, todo = s.split(b'e', 1)
            return (int(i[1:]), todo)
        except (ValueError, TypeError):
            raise BcodeError("Not an integer %r" % s)
    elif s[0:1] in [b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9']:
        try:
            length, string = s.split(b':', 1)
            length = int(length)
            return (string[0:length], string[length:])
        except (ValueError, TypeError):
            raise BcodeError("Not a string %r" % s)
    elif s[0:1] == b'l':
        l = []
        try:
            if s[1:2] == b"e":
                return (l, s[2:])
            item, todo = _bdecode(s[1:])
            l.append(item)
            while todo[0:1] != b"e":
                item, todo = _bdecode(todo)
                l.append(item)
            return (l, todo[1:])
        except (ValueError, TypeError, IndexError):
            raise BcodeError("Not a list %r" % s)
    elif s[0:1] == b'd':
        d = {}
        try:
            if s[1:2] == b"e":
                return d, s[2:]
            key, todo = _bdecode(s[1:])
            if todo[0:1] == b"e":
                raise BcodeError("Not bencoded string")
            value, todo = _bdecode(todo)
            d[key] = value
            while todo[0:1] != b"e":
                key, todo = _bdecode(todo)
                if todo[0:1] == b"e":
                    raise BcodeError("Not bencoded string")
                #print(todo)
                value, todo = _bdecode(todo)
                d[key] = value
            return (d, todo[1:])
        except (ValueError, TypeError, IndexError) as e:
            raise BcodeError("Not a dict %r\n%r" % (s, e))
    else:
        raise BcodeError("Not bencoded string %s" % s)

