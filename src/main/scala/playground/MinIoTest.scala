package playground

import org.apache.spark.sql._

object MinIoTest extends App{

  val spark = SparkSession
    .builder()
    .appName("comment-analyzer")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  spark.sparkContext.hadoopConfiguration .set("fs.s3a.endpoint", "http://localhost:9000")
  spark.sparkContext.hadoopConfiguration .set("fs.s3a.access.key", "minio123")
  spark.sparkContext.hadoopConfiguration .set("fs.s3a.secret.key", "minio123")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
  spark.sparkContext.hadoopConfiguration .set("fs.s3a.connection.ssl.enabled","false")

  import spark.implicits._

  val path = "s3a://minio/testbucket"

  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  val rdd = spark.sparkContext.parallelize(data)

  val dfFromRDD1 = rdd.toDF()
  dfFromRDD1.printSchema()
  dfFromRDD1.show()

  dfFromRDD1.write.csv(path)

}
