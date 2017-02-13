import pymongo

import config


def getdb(collection="torrents_data"):
    try:
        return getdb.db[collection]
    except AttributeError:
        db = pymongo.MongoClient(config.mongo["host"], config.mongo["port"])[config.mongo["db"]] 
        if config.mongo.get("user"):
            db.authenticate(config.mongo.get("user"), config.mongo.get("pwd"), mechanism='SCRAM-SHA-1')
        getdb.db = db
        return db[collection]
