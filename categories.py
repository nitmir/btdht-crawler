#!/usr/bin/env python
import re
import os
import mimetypes

categories = [
    'video',
    'audio',
    'archive',
    'document',
    'software',
    'image',
    'other',
]

rar_part_re = re.compile("^\.r[0-9]+$")


def guess(filename):
    typ = None
    mime =  mimetypes.guess_type(filename, strict=False)[0]
    if mime:
        typ = mime_to_category(mime)
    if typ is None:
        ext = os.path.splitext(filename)[1]
        if ext:
            typ = extension_to_category(ext)
    return typ

def extension_to_category(ext):
    ext = ext.lower()
    if ext in {
        '.m2ts', '.clpi', '.vob', '.rmvb', '.ifo', '.bup', '.cdg', '.m4v', '.bdmv', '.bik', '.f4v', '.idx', '.vp6', '.ogm', '.divx', '.mpg', '.avi',
        '.m2v', '.tp', '.ratdvd', '.iva', '.m2t'
    }:
        return 'video'
    elif ext in {
        '.pak', '.arc', '.sub', '.ccd', '.accurip', '.img', '.vghd', '.psarc', '.wv', '.tta', '.mds', '.gz', '.msu', '.pimx', '.mdf', '.pima', '.package',
        '.pck', '.cso', '.sfz', '.wbfs', '.ova', '.xz', '.bz2', '.vpk', '.nrg'
    }:
        return 'archive'
    elif ext in {
        '.ncw', '.nki', '.ape', '.mka', '.ac3', '.m4b', '.sb', '.exs', '.tak', '.wem', '.m4r', '.fsb', '.cue', '.rx2', '.shn', '.sdat', '.nkm', '.aac', '.nmsv',
        '.at3', '.pcm', '.nkc', '.swa', '.nkx', '.m4p', '.dtshd', '.ksd', '.dts'
    }:
        return 'audio'
    elif ext in {'.dds', '.tga', '.webp', '.raw', '.abr', '.max', '.wmf', '.xm', '.ipl', '.pam'}:
        return 'image'
    elif ext in {'.nfo', '.epub', '.log', '.mobi', '.lit', '.azw3', '.prc', '.asd', '.vtx', '.fb2', '.cas', '.md', '.ps3'}:
        return 'document'
    elif ext in {
        '.php', '.lua', '.cmd', '.opa', '.pyd', '.cs', '.strings', '.res', '.properties', '.manifest', '.nib', '.mo', '.rpyc', '.rpy', '.x32', '.fda', '.mui'
        '.nds', '.fx', '.prg', '.rsrc', '.scss', '.dmt', '.catalyst', '.pkg', '.bin', '.so', '.sql', '.man', '.mui', '.nds', '.qm', '.3ds', '.chd', '.inf'
    }:
        return 'software'
    elif ext in {
        '.sldprt', '.url', '.mpls', '.ass', '.dat', '.ini', '.db', '.xrm-ms', '.xxx', '.upk', '.mst', '.fxp', '.ans', '.opal', '.w3x', '.zdct', '.ff', '.gmp',
        '.fbl', '.map', '.md5', '.dcp', '.reg', '.lrtemplate', '.lmk', '.bc!', '.assets', '.poi', '.gp3', '.gp4', '.3dl', '.toc', '.diz', '.cfg', '.nka', '.smc', '.lim',
        '.nm2', '.lng', '.amt', '.big', '.paz', '.h2p', '.ssa', '.szs', '.xnb', '.dwg', '.ide', '.sys', '.index', '.3dc', '.rlf', '.lst', '.ftr', '.ozf2', '.sxt', '.ipa',
        '.nes', '.data', '.fxb', '.bndl', '.lyc', '.smarch', '.bfdca', '.sims3pack', '.fuz', '.fpa', '.fsp', '.sdfdata', '.meta', '.bk2', '.unity3d', '.nkp', '.dsf', '.loc',
        '.lnk', '.nksn', '.lzarc', '.mpq', '.plist', '.hdr', '.gmspr', '.avs', '.rxdata', '.bnk', '.rvdata2', '.sabs', '.pz2', '.w3m', '.bsp', '.msp', '.sse', '.aep',
        '.efd', '.ngrr', '.rpym', '.dff', '.obf', '.unr', '.sba', '.ffp', '.nm7', '.rpymc', '.jcd', '.pkz', '.vdb', '.fxc', '.grir', '.dylib', '.gpx', '.dl_', '.pub', '.txd',
        '.sgdt', '.s', '.afpk', '.cmp', '.atw', '.gp5', '.sabl', '.cci', '.smd', '.config', '.mcd', '.prp', '.ifs', '.dmp', '.pxs', '.icc', '.icns', '.yrdm', '.prt_omn', '.sob',
        '.rwd', '.sgo', '.torrent', '.key', '.ttf', '.sig', '.otf', '.m3u8', '.pac', '.npk', '.ph'
    }:
        return 'other'
    elif rar_part_re.match(ext):
        return 'archive'
    else:
        return None
    

def mime_to_category(mime):
    typ, sub_typ = mime.split('/')
    sub_typ = sub_typ.lower()
    if typ == "video":
        return 'video'
    elif typ == "audio":
        return "audio"
    elif typ == "image":
        return "image"
    elif typ in {"model", "message", "chemical"}:
        return 'document'
    elif typ == "text":
        if sub_typ in {
            'vnd.dmclientscript', 'x-c++hdr', 'x-c++src', 'x-chdr', 'x-crontab',
            'x-csh', 'x-csrc', 'x-java', 'x-makefile', 'x-moc', 'x-pascal', 'x-pcs-gcd',
            'x-perl', 'x-python', 'x-sh', 'x-tcl', 'x-dsrc', 'x-haskell', 'x-literate-haskell',
        }:
            return 'software'
        elif sub_typ in {"vnd.abc", "x-lilypond"}:
            return "audio"
        else:
            return 'document'
    elif typ == "application":
        if sub_typ in {"dicom"}:
            return "image"
        elif sub_typ in {
            "ecmascript", "java-archive", "javascript", "java-vm", "vnd.android.package-archive",
            "x-debian-package", "x-msdos-program", "x-msi", "x-python-code", "x-redhat-package-manager",
            "x-ruby", "x-shockwave-flash", "x-silverlight", 'x-cab', 'x-sql'
        }:
            return "software"
        elif sub_typ in {
            "gzip", "rar", "x-7z-compressed", "x-apple-diskimage",
            "x-iso9660-image", "x-lha", "x-lzh", "x-gtar-compressed", "x-tar", "zip"
        }:
            return 'archive'
        elif sub_typ in {"json", "msword", "oebps-package+xml", "onenote", "pdf", "postscript", "rtf", "smil+xml", "x-abiword", "x-hdf", "x-cbr", "x-cbz"}:
            return 'document'
        elif any(sub_typ.startswith(t) for t in ["vnd.ms-", "vnd.oasis.opendocument", "vnd.openxmlformats-officedocument", "vnd.stardivision", "vnd.sun.xml"]):
            return 'document'
        else:
            return None
    elif mime == "x-epoc/x-sisx-app":
        return 'software'
    else:
        return None
