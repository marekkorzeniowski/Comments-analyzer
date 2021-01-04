CREATE EXTERNAL TABLE  sample_db.posts_parquet (
rowKey STRING,
postId BIGINT,
postTypeId BIGINT,
parentId BIGINT,
creationDateTime TIMESTAMP,
score BIGINT,
viewCount BIGINT,
title STRING,
sentiment_label STRING,
avg_score FLOAT,
pos_score INTEGER,
neg_score INTEGER,
tot_score INTEGER,
body STRING,
ownerUserId BIGINT,
tags STRING,
answerCount BIGINT,
commentCount BIGINT,
favoriteCount BIGINT,
userName STRING,
location STRING
)
STORED AS PARQUET
LOCATION 's3://spark-marek-test-data/results/posts_stream/'
tblproperties ("parquet.compress"="SNAPPY");