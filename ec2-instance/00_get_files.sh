#!/bin/bash
URL=https://raw.githubusercontent.com/marekkorzeniowski/Comments-analyzer/master/ec2-instance/

declare -a files=(Comments.xml
                  Posts.xml
                  01_get_kafka_and_start_zookeeper.sh
                  02_start_kafka_server.sh
                  03_create_topic_and_start_consumer.sh
                  04_create_second_topic.sh)

for file in "${files[@]}"
do
   wget $URL$file
done

chmod +x *
