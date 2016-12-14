#!/bin/bash

SCALA_VERSION=2.11
KAFKA_VERSION=0.10.1.0
DEBIAN_FRONTEND=noninteractive

echo "127.0.1.1 ubuntu-xenial" >> /etc/hosts

echo updating and adding repositories
#apt-get install -y software-properties-common
#add-apt-repository -y ppa:brightbox/ruby-ng

#java 8
#echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
#add-apt-repository -y ppa:webupd8team/java

apt-get update && apt-get install -y ca-certificates apt-transport-https vim curl linux-image-extra-$(uname -r) openjdk-8-jre redis-tools
	#ruby2.3 ruby2.3-dev \

curl https://get.docker.com | sh > /dev/null
usermod ubuntu -a -G docker
sed -i 's/\"set background=dark/set background=dark/' /etc/vim/vimrc

#kafka
curl -SL http://www-eu.apache.org/dist/kafka/$KAFKA_VERSION/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz | tar xz -C /opt
ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}/ /opt/kafka

mkdir -p /var/log/zookeeper /var/log/kafka /usr/lib/systemd/system /opt/cassandra-init

sed -i 's@#listeners=PLAINTEXT://:9092@listeners=PLAINTEXT://10.0.0.3:9092@' /opt/kafka/config/server.properties
sed -i 's@#delete.topic.enable=true@delete.topic.enable=true@' /opt/kafka/config/server.properties
echo "auto.create.topics.enable=true" >> /opt/kafka/config/server.properties

#docker build -t kafka /dockerfiles/kafka-zookeeper && docker run -d -p 2181:2181 -p 9092:9092 kafka 

mv /tmp/*service /usr/lib/systemd/system
mv /tmp/cassandra.init /opt/cassandra-init

#Redis

#Prepare
echo "vm.overcommit_memory = 1" > /etc/sysctl.conf
sysctl vm.overcommit_memory=1

echo 'net.core.somaxconn = 65535' >> /etc/sysctl.conf
sysctl -w net.core.somaxconn=65535

echo never > /sys/kernel/mm/transparent_hugepage/enabled


#Run
docker run --name redis -d -v /var/lib/redis:/data -p 6379:6379 redis:alpine redis-server --appendonly yes

systemctl enable zookeeper kafka docker-containers transparent_hugepage
systemctl start zookeeper kafka

docker run -d -v /var/lib/cassandra:/var/lib/cassandra -v /opt/cassandra-init:/opt/cassandra-init -p 9042:9042 --name cassandra cassandra

#let the services start
sleep 60

#Create a topic
/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic boostedgg 

#Create cassandra keyspace and tables
docker exec cassandra cqlsh -f /opt/cassandra-init/cassandra.init

