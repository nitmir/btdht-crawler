-- MySQL dump 10.13  Distrib 5.5.41, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: piratebay
-- ------------------------------------------------------
-- Server version	5.5.41-0ubuntu0.14.10.1-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `torrents`
--

DROP TABLE IF EXISTS `torrents`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `torrents` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `description` text COLLATE utf8_unicode_ci,
  `category_id` tinyint(4) DEFAULT NULL,
  `size` bigint(20) unsigned DEFAULT NULL,
  `hash` varchar(40) CHARACTER SET ascii NOT NULL,
  `files_count` int(11) DEFAULT '0',
  `created_at` datetime DEFAULT NULL,
  `torrent_status` smallint(2) DEFAULT '0',
  `visible_status` smallint(2) DEFAULT '0',
  `downloads_count` mediumint(8) unsigned NOT NULL DEFAULT '0' COMMENT 'umax = 16777215',
  `scrape_date` datetime DEFAULT NULL,
  `seeders` mediumint(8) unsigned NOT NULL DEFAULT '0',
  `leechers` mediumint(8) unsigned NOT NULL DEFAULT '0',
  `tags` varchar(500) COLLATE utf8_unicode_ci DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `dht_last_get` timestamp NULL DEFAULT NULL,
  `dht_last_announce` timestamp NULL DEFAULT NULL,
  `torcache` tinyint(1) DEFAULT NULL,
  `torcache_notfound` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `hash` (`hash`),
  KEY `created_at` (`created_at`),
  KEY `size` (`size`),
  KEY `category_id_torrent_status_visible_status` (`category_id`,`torrent_status`,`visible_status`),
  KEY `dht_last_get` (`dht_last_get`),
  KEY `dht_last_announce` (`dht_last_announce`),
  KEY `torcache` (`torcache`),
  KEY `torcache_notfound` (`torcache_notfound`),
  KEY `scrape_date` (`scrape_date`)
) ENGINE=MyISAM AUTO_INCREMENT=17824994 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ROW_FORMAT=COMPACT;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2015-02-06 18:34:24
