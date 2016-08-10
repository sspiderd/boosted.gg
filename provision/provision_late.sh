mv /tmp/zookeeper.conf /etc/init
mv /tmp/kafka.conf /etc/init

service zookeeper start
service kafka start

#Create a topic
/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mastergg
