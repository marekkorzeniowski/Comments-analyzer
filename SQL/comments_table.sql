CREATE EXTERNAL TABLE  sample_db.comments_parquet (
rowKey STRING,
commentId BIGINT,
postId BIGINT,
score INTEGER,
sentiment_label STRING,
avg_score FLOAT,
pos_score INTEGER,
neg_score INTEGER,
tot_score INTEGER,
text STRING,
creationDate TIMESTAMP,
userId BIGINT,
userName STRING,
location STRING
  )
STORED AS PARQUET
LOCATION 's3://spark-marek-test-data/results/comments_stream/'
tblproperties ("parquet.compress"="SNAPPY");