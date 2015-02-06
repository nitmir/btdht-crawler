ALTER TABLE torrents ADD COLUMN `dht_last_get` timestamp NULL AFTER `updated_at`;
ALTER TABLE torrents ADD COLUMN `dht_last_announce` timestamp NULL AFTER `dht_last_get`;
ALTER TABLE torrents ADD COLUMN `torcache` tinyint(1) NULL AFTER `dht_last_announce`;
ALTER TABLE torrents ADD COLUMN `torcache_notfound` tinyint(1) NULL AFTER `torcache`;
CREATE INDEX `dht_last_get` ON torrents(`dht_last_get`);
CREATE INDEX `dht_last_announce` ON torrents(`dht_last_announce`);
CREATE INDEX `torcache` ON torrents(`torcache`);
CREATE INDEX `torcache_notfound` ON torrents(`torcache_notfound`);
