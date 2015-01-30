# -*- coding: utf-8 -*-
# mysql credentials
mysql = {
  "user": "piratebay",
  "passwd": "PASSWORD",
  "db":"piratebay",
  "host":"localhost"
}

# crawler base port. port will be choosen (deterministically)
# between crawler_base_port and crawler_base_port + 255
crawler_base_port = 12345

# power of 2 if the number of dht instance de lauch while crawling
# zb 4 for 2^4=16 instances
crawler_instance = 3  # 8 instances

# max virtual memory in byte
crawler_max_memory = 4 * 1024 * 1024 * 1024 # 4GB

torrents_dir = "torrents/"
torrents_done = "torrents_done/"
torrents_archive = "torrents_archives/"
torrents_new = "torrents_new/"

public_ip = "1.2.3.4"
