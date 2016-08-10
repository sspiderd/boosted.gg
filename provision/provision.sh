#!/bin/bash


SCALA_VERSION=2.11
KAFKA_VERSION=0.10.0.0
DEBIAN_FRONTEND=noninteractive

echo updating and adding repositories
apt-get install -y software-properties-common
add-apt-repository -y ppa:brightbox/ruby-ng

#java 8
echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
add-apt-repository -y ppa:webupd8team/java

apt-get update && apt-get install -y ca-certificates apt-transport-https vim curl linux-image-extra-$(uname -r) ruby2.3 ruby2.3-dev rake ansible subversion oracle-java8-installer oracle-java8-set-default maven

curl https://get.docker.com | sh > /dev/null
usermod vagrant -a -G docker
sed -i 's/\"set background=dark/set background=dark/' /etc/vim/vimrc


#kafka
curl -SL http://www-eu.apache.org/dist/kafka/$KAFKA_VERSION/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz | tar xz -C /opt
ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}/ /opt/kafka

mkdir -p /var/log/zookeeper /var/log/kafka

sed -i 's@#listeners=PLAINTEXT://:9092@listeners=PLAINTEXT://10.0.0.3:9092@' /opt/kafka/config/server.properties



#docker build -t kafka /dockerfiles/kafka-zookeeper && docker run -d -p 2181:2181 -p 9092:9092 kafka 
