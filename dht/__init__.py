import pyximport
pyximport.install()

from .dht import  DHT, DHT_BASE, ID, Node, Bucket, RoutingTable, NotFound, BucketFull, NoTokenError
