# -*- coding: utf-8 -*-

# 0 no debug, 100 full debug
debug = 0

mongo = {
  "db": "btdht-crawler",
}

# crawler udp base port. port will be choosen (deterministically)
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

# directory use to store tracker full scrape files
torrents_scrape = "torrents_scrape/"
