mv /tmp/zookeeper.conf /etc/init
mv /tmp/kafka.conf /etc/init
mv /tmp/docker-containers.conf /etc/init

service zookeeper start
service kafka start

docker run -d -v /var/lib/cassandra:/var/lib/cassandra -p 9042:9042 --name cassandra cassandra

#let the service start
sleep 5

#Create a topic
/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic boostedgg
