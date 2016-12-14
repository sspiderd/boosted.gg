vagrant ssh -c "/opt/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --delete --topic boostedgg"
echo "You have reset the kafka topic, don't forget to delete the spark checkpoint directory!"
