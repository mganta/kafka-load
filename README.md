Sample setup to run kafka producer & consumer



set JVM Heaps

  export _JAVA_OPTIONS="-Xms2048m -Xmx20480m"



Create a topic

  kafka-topics.sh --zookeeper zkvm0:2181,zkvm1:2181,zkvm2:2181 --create --topic test --partitions 64 --replication-factor 3



run producer

  java -cp kafka-load-1.0-SNAPSHOT-jar-with-dependencies.jar com.msft.kafka.ProducerLoadTest --topic test --num-records 1000000 --record-size 1024 --producer-props bootstrap.servers=bk0:9092,bkvm1:9092,bkvm2:9092,bkvm3:9092,bkvm4:9092,bkvm5:9092,bkvm6:9092,bkvm7:9092,bkvm8:9092,bkvm9:9092,bkvm10:9092,bkvm11:9092 buffer.memory=25165824 compression.type=none batch.size=1250000 linger.ms=30 max.request.size=2097152 --throughput 100 --threads 500



run consumer

   java -cp kafka-load-1.0-SNAPSHOT-jar-with-dependencies.jar com.msft.kafka.ConsumerLoadTest --zookeeper zkvm0:2181,zkvm1:2181,zkvm2:2181 --new-consumer --show-detailed-stats --threads 30 --topic test --broker-list bkvm0:9092,bkvm1:9092,bkvm2:9092,bkvm3:9092,bkvm4:9092,bkvm5:9092,bkvm6:9092,bkvm7:9092,bkvm8:9092,bkvm9:9092,bkvm10:9092,bkvm11:9092 --messages 9000000 --num-fetch-threads 25
