#!/bin/bash

# install kafka-python
pip3 install kafka-python
# download
wget http://ftp.man.poznan.pl/apache/kafka/2.5.0/kafka_2.12-2.5.0.tgz
# unpack
tar -xzf kafka_2.12-2.5.0.tgz
# remove
rm kafka_2.12-2.5.0.tgz
#ZMIEŃ KATALOG
cd kafka_2.12-2.5.0
#Start zookeepera
bin/zookeeper-server-start.sh config/zookeeper.properties