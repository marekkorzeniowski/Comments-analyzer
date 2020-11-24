#ZMIEŃ KATALOG
cd kafka_2.12-2.5.0
#WYKONAJ - terminal 3 - stworzenie tematu comment-analyzer
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic post-analyzer
#WYKONAJ - terminal 3 - consumer - odbieranie wiadomości
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic post-analyzer --from-beginning