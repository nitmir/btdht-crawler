openbay crawler
===============

A bittorrent mainline dht crawler that fetch torrents files via the
[magnet system](http://www.bittorrent.org/beps/bep_0009.html) or torcache.
It runs multiple instance of the dht concurrently to be able to feed a database
from the torrents currently downloaded within the dht.

## dependancies
  * python-progressbar
  * python-zmq
  * python-mysqldb
  * python-chardet
  * python-psutil
  * btdht (linked as submodule)
  * cython (for btdht)
  * datrie (for btdht, linked as submodule)

## usage

First, you'll need to compile the btdht and datrie modules. Go in the datrie
subdirectory and run `./setup.py build`. Do the same in the python-btdht
subdirectory.

Copy `config_sample.py` to `config.py` and edit to reflect your db settings.
Make sure your torrents table has all the needed columns. You can use `sql/update.sql`
to update your torrents table from an openbay install. Otherwise, use `sql/schema.sql`
to create it. Beware that `sql/schema.sql` will destroy existing torrents table before
recreating it.

Then create the directories `torrents_dir`, `torrents_done`, `torrents_archive`, and 
`torrents_new` as you specified them in `config.py`

Run `./crawler.py` to start crawling the dht and `./feed.py` to feed the database
with discovered torrents.

Moreother `./feed.py` will send the hash of new discovered torrents to others 
running `./feed.py` and scrape torrents agains udp://open.demonii.com:1337

## Notes

`./feed.py` will insert the torrents in the 'other' category using the torrent
name as name, the list of files as description and try to upload it on torcache.
