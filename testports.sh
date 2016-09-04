#!/bin/bash -e

SERVER=10.0.0.3

</dev/tcp/$SERVER/6379 && echo Redis \(6379\) Open || echo Redis \(6379\) Closed
</dev/tcp/$SERVER/9042 && echo Cassandra \(9042\) Open || echo Cassandra \(9042\) Closed
</dev/tcp/$SERVER/9092 && echo Kafkush \(9042\) Open || echo Kafkush \(9042\) Closed
</dev/tcp/$SERVER/2181 && echo ZooKeeper \(2181\) Open || echo ZooKeeper \(2181\) Closed
