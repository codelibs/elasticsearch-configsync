Elasticsearch Config Sync Plugin
=======================

## Overview

Config Sync Plugin provides a feature to distribute files, such as script or dictionary file, to nodes in your cluster.
These files are managed in .configsync index, and each node sync up with them.

## Version

| Version   | Tested on Elasticsearch |
|:---------:|:-----------------------:|
| master    | 2.1.X                   |
| 2.1.0     | 2.1.0                   |
| 2.0.0     | 2.0.0                   |
| 1.7.0     | 1.7.2                   |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-configsync/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### Install Config Sync Plugin

    $ $ES_HOME/bin/plugin install org.codelibs/elasticsearch-configsync/2.1.0

## Getting Started

### Register File

    $ curl -XPOST localhost:9200/_configsync/file?path=user-dict.txt -d @user-dict.txt

The above request is to add file info to .configsync index.
path parameter is a synced file location under $ES_CONF directory(ex. /etc/elasticsearch/user-dict.txt).

### Get File List

Send GET request without path parameter:

    $ curl -XGET localhost:9200/_configsync/file
    {"acknowledged":true,"path":["user-dict.txt"]}

### Get File

Send GET request with path parameter:

    $ curl -XGET localhost:9200/_configsync/file?path=user-dict.txt

### Delete File

Send DELETE request with path parameter:

    $ curl -XDELETE localhost:9200/_configsync/file?path=user-dict.txt

### Sync

Each node copies a file from .configsync index periodically if the file is updated.
The interval time is specified by configsync.flush_interval in /etc/elasticserch/elasticsearch.yml.
The default value is 1m.

    configsync.flush_interval: 1m
