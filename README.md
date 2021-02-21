Elasticsearch Config Sync
=======================

## Overview

Config Sync provides a feature to distribute files, such as script or dictionary file, to nodes in your cluster.
These files are managed in .configsync index, and each node sync up with them.

## Installation

TBD

## Getting Started

### Register File

    $ curl -XPOST -H 'Content-Type:application/json' localhost:9299/_configsync/file?path=user-dict.txt --data-binary @user-dict.txt

The above request is to add file info to .configsync index.
path parameter is a synced file location under $ES_CONF directory(ex. /etc/elasticsearch/user-dict.txt).

### Get File List

Send GET request without path parameter:

    $ curl -XGET -H 'Content-Type:application/json' localhost:9299/_configsync/file
    {"acknowledged":true,"path":["user-dict.txt"]}

### Get File

Send GET request with path parameter:

    $ curl -XGET -H 'Content-Type:application/json' localhost:9299/_configsync/file?path=user-dict.txt

### Delete File

Send DELETE request with path parameter:

    $ curl -XDELETE -H 'Content-Type:application/json' localhost:9299/_configsync/file?path=user-dict.txt

### Sync

Each node copies a file from .configsync index periodically if the file is updated.
The interval time is specified by configsync.flush_interval in /etc/elasticserch/elasticsearch.yml.
The default value is 1m.

    configsync.flush_interval: 1m

### Reset

To restart a scheduler for checking .configsync index, send POST request as below:

    $ curl -XPOST -H 'Content-Type:application/json' localhost:9299/_configsync/reset
