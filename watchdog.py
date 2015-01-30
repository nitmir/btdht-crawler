#!/usr/bin/env python
import os
import sys
import psutil
import time

process = {}
def get_process(file):
    if file in process and process[file].is_running():
        return process[file]
    else:
        try:
            pid = int(open(file).read().strip())
            process[file] = psutil.Process(pid)
            return process[file]
        except( psutil.NoSuchProcess, ValueError, IOError) as e:
            #print("%s not found: %s" % (file, e))
            return None


def loop():
    pidfiles = sys.argv[1:]
    print pidfiles
    stats={}
    if not pidfiles:
        return
    while True:
        for file in pidfiles:
            p = get_process(file)
            if p:
                try:
                    c = p.io_counters()
                    if file in stats:
                        if c[0] != stats[file][0] or c[1] != stats[file][1]:
                            stats[file] = (c[0], c[1], time.time(), 0)
                        elif time.time() - stats[file][2] > 120:
                            print("%s no activity since 30s, killing" % file)
                            if stats[file][3] < 2:
                                os.system("kill %s" % p.pid)
                                stats[file]=stats[file][0:3] + (stats[file][3] + 1, )
                            elif stats[file][3] < 4:
                                os.system("kill -15 %s" % p.pid)
                                stats[file]=stats[file][0:3] + (stats[file][3] + 1, )
                            else:
                                os.system("kill -9 %s" % p.pid)
                                stats[file]=stats[file][0:3] + (stats[file][3] + 1, )
                    else:
                        stats[file] = (c[0], c[1], time.time(), 0)
                except (psutil.AccessDenied, psutil.NoSuchProcess):
                    pass
            else:
                if file in stats:
                    del stats[file]
        time.sleep(1)


if __name__ == '__main__':
    loop()
