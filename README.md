Elasticsearch Config Sync Module
[![Java CI with Maven](https://github.com/codelibs/elasticsearch-configsync/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/elasticsearch-configsync/actions/workflows/maven.yml)
=======================

## Overview

Config Sync Module provides a feature to distribute files, such as script or dictionary file, to nodes in your cluster.
These files are managed in .configsync index, and each node sync up with them.

## Version

[Versions in Maven Repository](https://repo1.maven.org/maven2/org/codelibs/elasticsearch-configsync/)

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-configsync/issues "issue").

## Installation

### 7.11 -

    $ CONFIGSYNC_URL=https://repo.maven.apache.org/maven2/org/codelibs/elasticsearch-configsync/7.11.0/elasticsearch-configsync-7.11.0.zip
    $ curl -o /tmp/configsync.zip $CONFIGSYNC_URL
    $ mkdir -p /usr/share/elasticsearch/modules/configsync
    $ unzip -d /usr/share/elasticsearch/modules/configsync /tmp/configsync.zip

### - 7.10

    $ $ES_HOME/bin/elasticsearch-plugin install org.codelibs:elasticsearch-configsync:7.10.0

## Getting Started

### Register File

    $ curl -XPOST -H 'Content-Type:application/json' localhost:9200/_configsync/file?path=user-dict.txt --data-binary @user-dict.txt

The above request is to add file info to .configsync index.
path parameter is a synced file location under $ES_CONF directory(ex. /etc/elasticsearch/user-dict.txt).

### Get File List

Send GET request without path parameter:

    $ curl -XGET -H 'Content-Type:application/json' localhost:9200/_configsync/file
    {"acknowledged":true,"path":["user-dict.txt"]}

### Get File

Send GET request with path parameter:

    $ curl -XGET -H 'Content-Type:application/json' localhost:9200/_configsync/file?path=user-dict.txt

### Delete File

Send DELETE request with path parameter:

    $ curl -XDELETE -H 'Content-Type:application/json' localhost:9200/_configsync/file?path=user-dict.txt

### Sync

Each node copies a file from .configsync index periodically if the file is updated.
The interval time is specified by configsync.flush_interval in /etc/elasticserch/elasticsearch.yml.
The default value is 1m.

    configsync.flush_interval: 1m

### Reset

To restart a scheduler for checking .configsync index, send POST request as below:

    $ curl -XPOST -H 'Content-Type:application/json' localhost:9200/_configsync/reset
