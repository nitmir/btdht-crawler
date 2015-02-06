# -*- coding: utf-8 -*-
# mysql credentials
mysql = {
  "user": "piratebay",
  "passwd": "PASSWORD",
  "db":"piratebay",
  "host":"localhost",
  "charset":'utf8',
}

# crawler base port. port will be choosen (deterministically)
# between crawler_base_port and crawler_base_port + 255
# if you lauch multiple worker it may happen that the first
# byte of the id are equals beetween 2 worken
# then you will have an error saying port already in use
# just remove one of the crawler%d.id and restart
crawler_base_port = 12345

# number of process to span for crawling the dht
# one by proc is good, more is useless, increase
# the instance number instead
crawler_worker = 1

# power of 2 if the number of dht instance de lauch by worker
# while crawling: 4 is for 2^4=16 instances
crawler_instance = 3  # 8 instances

# max resident memory in byte
crawler_max_memory = 8 * 1024 * 1024 * 1024 # 8GB

# where to write torrents retreived from dht or torcache
torrents_dir = "torrents/"
# where to move processed torrents
torrents_done = "torrents_done/"
# where to archive torrent. The script will create
# one subdirectory by day in this directory
torrents_archive = "torrents_archives/"

# If the torrent has successfully been uploaded to torcache
# the liste of pieces will be removed in the torrent file
# before archiving. It allow to keep all the interesting
# metadata of the .torrent (name, list of file) but drastically
# reduce its size. The file will be unusable in a torrent client
compact_archived_torrents = True

# which dir to watch for new torrent
# this is here you can had custom torrent by hand
# juste by coping them in that ddirectory
torrents_new = "torrents_new/"

# Your public ipv4 address
public_ip = "1.2.3.4"

# some ports for the replicatiob system
# beetween user running feed.py
# the first founding a torrent in the dht will inform
# the other with is torcache link
replication_udp_port = 5004
replication_tcp_port = 5004
replication_dht_port = 5005

# Scrape intervale
scrape_interval = 60 # in minute, set 0 to disable scraping

# Max number of torrent to scrape by pass
# set a number high enough to be able de scrape every torrents in a
# scrape_interval. a pass take 5min so scrape_interval should be
# something like num_torrents / (scrape_interval / 5)
# Set to None to scrape all torrents that need to be scraped
scape_limit = 250000 # set to None to disable
