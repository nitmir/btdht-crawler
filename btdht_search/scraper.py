import urllib
import socket
import random
import struct
import select
import time
import collections
from btdht.utils import bdecode, BcodeError
from requests_futures.sessions import FuturesSession
from requests.exceptions import RequestException
from requests.adapters import HTTPAdapter
try:
    from urllib.parse import urlparse, urlunsplit
except ImportError:
    from urlparse import urlparse, urlunsplit


def scrape_max(trackers, hashes, udp_timeout=1, tcp_timeout=1, tcp_max_workers=None):
    results = scrape(trackers, hashes, udp_timeout=udp_timeout, tcp_timeout=tcp_timeout, tcp_max_workers=tcp_max_workers)
    results_trackers = set(results.keys())

    scrape_result = {}
    for hash in hashes:
        scrape_result[hash] = {'complete': -1, 'peers': -1, 'seeds': -1}
    for result in results.values():
        for hash in result:
            try:
                scrape_result[hash]['complete'] = max(
                    scrape_result[hash]['complete'],
                    result[hash]['complete']
                )
                scrape_result[hash]['peers'] = max(
                    scrape_result[hash]['peers'],
                    result[hash]['peers']
                )
                scrape_result[hash]['seeds'] = max(
                    scrape_result[hash]['seeds'],
                    result[hash]['seeds']
                )
            except KeyError:
                pass
    # delete hash where no results where returned
    for hash in hashes:
        if (
            scrape_result[hash]['seeds'] < 0 or
            scrape_result[hash]['peers'] < 0 or
            scrape_result[hash]['complete'] < 0
        ):
            del scrape_result[hash]
    return (results_trackers, scrape_result)


def scrape(trackers, hashes, udp_timeout=1, tcp_timeout=1, tcp_max_workers=None):
    """
    Returns the list of seeds, peers and downloads a torrent info_hash has,
    according to the specified trackers

    Args:
        trackers (list): The announce url for trackers, usually taken directly
            from the torrent metadata
        hashes (list): A list of torrent info_hash's to query the tracker for

    Returns:
        A dict of dicts of dicts. The key is the tracker, the value is a dict with the torrent
        info_hash's from the 'hashes' parameter as key, and a dict containing "seeds", "peers"
        and "complete" as value
        Eg:
        {
            "udp://tracker.example.com:80": {
                "2d88e693eda7edf3c1fd0c48e8b99b8fd5a820b2" : {
                    "seeds" : "34", "peers" : "189", "complete" : "10"
                },
                "8929b29b83736ae650ee8152789559355275bd5c" : {
                    "seeds" : "12", "peers" : "0", "complete" : "290"
                }
            }
        }
    """
    trackers = [tracker.lower() for tracker in trackers]
    parsed_trackers = [(tracker, urlparse(tracker)) for tracker in trackers]
    udp_parsed_trackers = [tracker for tracker in parsed_trackers if tracker[1].scheme == "udp"]
    tcp_parsed_trackers = [
        tracker for tracker in parsed_trackers if tracker[1].scheme in {"http", "https"}
    ]
    results = {}
    if tcp_parsed_trackers:
        for (tracker, parsed_tracker) in tcp_parsed_trackers:
            if "announce" not in tracker:
                raise RuntimeError("%s doesnt support scrape" % tracker)
        tcp_parsed_trackers = [
            (tracker, urlparse(tracker.replace("announce", "scrape")))
            for (tracker, parsed) in tcp_parsed_trackers
        ]
        session = FuturesSession(max_workers=(len(tcp_parsed_trackers) if tcp_max_workers is None else tcp_max_workers))
        requests = scrape_http_requests(session, tcp_parsed_trackers, hashes, timeout=tcp_timeout)

    if udp_parsed_trackers:
        results.update(scrape_udp(udp_parsed_trackers, hashes, timeout=udp_timeout))

    if tcp_parsed_trackers:
        results.update(scrape_http_get_response(requests))

    return results


def scrape_udp(parsed_trackers, hashes, timeout=1):
    connection_ids = {}
    transaction_ids = {}
    results = collections.defaultdict(dict)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for (tracker, parsed_tracker) in parsed_trackers:
        try:
            conn = (socket.gethostbyname(parsed_tracker.hostname), parsed_tracker.port)

            # Get connection ID
            req, transaction_id = udp_create_connection_request(transaction_ids)
            sock.sendto(req, conn)
            transaction_ids[transaction_id] = (0, time.time(), tracker, 0)
        except socket.error:
            pass

    while len(transaction_ids) > 0:
        (rlist, _, _) = select.select([sock], [], [], timeout + 0.5)
        if rlist:
                buf, froms = sock.recvfrom(2048)
                (action, transaction_id) = udp_get_status(buf)
                if transaction_id in transaction_ids:
                    (status, _, tracker, hash_id) = transaction_ids[transaction_id]
                    del transaction_ids[transaction_id]
                    hashes_to_scrape = None
                    # 0 for connection response
                    if status == action == 0:
                        connection_id = udp_parse_connection_response(buf, transaction_id)
                        connection_ids[tracker] = connection_id
                        # Scrape away
                        hashes_to_scrape = hashes[hash_id * 74: (hash_id+1) * 74]
                    # 2 for scrape response
                    elif status == action == 2:
                        try:
                            results[tracker].update(
                                udp_parse_scrape_response(buf, transaction_id, hashes)
                            )
                        except (RuntimeError, struct.error):
                            pass
                        hashes_to_scrape = hashes[hash_id * 74: (hash_id+1) * 74]
                    elif action == 3:
                        print("%s error: %s" % (tracker, udp_parse_error(buf, transaction_id)))
                    else:
                        print("Get other action != status = 2: %s == %s" % (action, status))
                    if hashes_to_scrape:
                        req, transaction_id = udp_create_scrape_request(
                            transaction_ids,
                            connection_ids[tracker],
                            hashes
                        )
                        try:
                            sock.sendto(req, froms)
                            transaction_ids[transaction_id] = (2, time.time(), tracker, hash_id+1)
                        except socket.error:
                            pass
        else:
            for transaction_id, (_, start_time, _, _) in transaction_ids.items():
                if time.time() - start_time > (timeout + 0.5):
                    del transaction_ids[transaction_id]

    return results


def scrape_http_requests(session, parsed_trackers, hashes, timeout=1):
    qs = []
    for hash in hashes:
        qs.append(("info_hash", hash))
    qs = urllib.urlencode(qs)
    requests = {}
    for (tracker, parsed_tracker) in parsed_trackers:
        pt = parsed_tracker
        url = urlunsplit((pt.scheme, pt.netloc, pt.path, qs, pt.fragment))
        requests[tracker] = session.get(url, timeout=timeout)
    return requests


def scrape_http_get_response(requests):
    results = {}
    for (tracker, future) in requests.items():
        try:
            response = future.result()
            if response.status_code == 200:
                decoded = bdecode(response.content)
                ret = {}
                for hash, stats in decoded.get('files', {}).iteritems():
                    s = stats["complete"]
                    p = stats["incomplete"]
                    c = stats["downloaded"]
                    ret[hash] = {"seeds": s, "peers": p, "complete": c}
                results[tracker] = ret
        except (RequestException, BcodeError, AttributeError) as error:
            pass
    return results


def udp_create_connection_request(transaction_ids):
    connection_id = 0x41727101980  # default connection id
    action = 0x0  # action (0 = give me a new connection id)
    transaction_id = udp_get_transaction_id(transaction_ids)
    # first 8 bytes is connection id, next 4 bytes is action, next 4 bytes is transaction id
    buf = struct.pack("!qii", connection_id, action, transaction_id)
    return (buf, transaction_id)


def udp_get_status(buf):
    if len(buf) < 8:
        raise RuntimeError("Response too short for getting transaction id: %r %s" % (buf, len(buf)))
    (action, transaction_id) = struct.unpack_from("!ii", buf)
    return (action, transaction_id)


def udp_parse_error(buf, sent_transaction_id):
    if len(buf) < 8:
        raise RuntimeError("Response too short for error msg: %r %s" % (buf, len(buf)))
    action, res_transaction_id = struct.unpack_from("!ii", buf)
    if res_transaction_id != sent_transaction_id:
        raise RuntimeError(
            "Transaction ID doesnt match in connection response! Expected %s, got %s" % (
                sent_transaction_id, res_transaction_id
            )
        )
    if action == 3:
        msg = buf[8:]
        return msg
    else:
        raise RuntimeError("Not and error response")


def udp_parse_connection_response(buf, sent_transaction_id):
    if len(buf) < 16:
        raise RuntimeError("Wrong response length getting connection id: %s" % len(buf))
    action = struct.unpack_from("!i", buf)[0]  # first 4 bytes is action

    res_transaction_id = struct.unpack_from("!i", buf, 4)[0]  # next 4 bytes is transaction id
    if res_transaction_id != sent_transaction_id:
        raise RuntimeError(
            "Transaction ID doesnt match in connection response! Expected %s, got %s" % (
                sent_transaction_id, res_transaction_id
            )
        )

    if action == 0x0:
        # unpack 8 bytes from byte 8, should be the connection_id
        connection_id = struct.unpack_from("!q", buf, 8)[0]
        return connection_id
    elif action == 0x3:
        error = struct.unpack_from("!s", buf, 8)
        raise RuntimeError("Error while trying to get a connection response: %s" % error)
    pass


def udp_create_scrape_request(transaction_ids, connection_id, hashes):
    action = 0x2  # action (2 = scrape)
    transaction_id = udp_get_transaction_id(transaction_ids)
    # first 8 bytes is connection id, next 4 bytes is action, followed by 4 byte transaction id
    buf = [struct.pack("!qii", connection_id, action, transaction_id)]
    # from here on, there is a list of info_hashes. They are packed as char[]
    for hash in hashes:
        buf.append(struct.pack("!20s", hash))
    return ("".join(buf), transaction_id)


def udp_parse_scrape_response(buf, sent_transaction_id, hashes):
    if len(buf) < 16:
        raise RuntimeError("Wrong response length while scraping: %s" % len(buf))
    action = struct.unpack_from("!i", buf)[0]  # first 4 bytes is action
    res_transaction_id = struct.unpack_from("!i", buf, 4)[0]  # next 4 bytes is transaction id
    if res_transaction_id != sent_transaction_id:
        raise RuntimeError(
            "Transaction ID doesnt match in scrape response! Expected %s, got %s" % (
                sent_transaction_id,
                res_transaction_id
            )
        )
    if action == 0x2:
        ret = {}
        offset = 8  # next 4 bytes after action is transaction_id, so data doesnt start till byte 8
        for hash in hashes:
            seeds = struct.unpack_from("!i", buf, offset)[0]
            offset += 4
            complete = struct.unpack_from("!i", buf, offset)[0]
            offset += 4
            leeches = struct.unpack_from("!i", buf, offset)[0]
            offset += 4
            ret[hash] = {"seeds": seeds, "peers": leeches, "complete": complete}
        return ret
    elif action == 0x3:
        # an error occured, try and extract the error string
        error = struct.unpack_from("!s", buf, 8)
        raise RuntimeError("Error while scraping: %s" % error)


def udp_get_transaction_id(transaction_ids):
    id = random.randrange(0, 2147483648)
    while id in transaction_ids:
        id = random.randrange(0, 2147483648)
    return id
