mv /tmp/zookeeper.conf /etc/init
mv /tmp/kafka.conf /etc/init

service zookeeper start
service kafka start


#let the service start
sleep 5

#Create a topic
/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mastergg
