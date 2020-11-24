lazy val root = (project in file(".")).
  settings(
    name := "Comments-analyzer",
    version := "1.0",
    scalaVersion := "2.12.10",
    mainClass in Compile := Some("Spark_App.Consumer")
  )

val sparkVersion = "3.0.0"
//val cassandraConnectorVersion = "2.4.2"
val kafkaVersion = "2.4.0"
val log4jVersion = "2.4.1"
val nlpLibVersion = "3.5.1"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion  % "provided",

  // streaming-kafka
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,

  // low-level integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVersion,


  // cassandra - this version officially works with Spark 2.4, but tested with Spark 3.0-preview as well
//  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,


  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  "edu.stanford.nlp" % "stanford-corenlp" % nlpLibVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % nlpLibVersion classifier "models",

  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,


  //spark-xml-parser
//  "com.databricks"%"spark-xml_2.12"%"0.6.0"

)

// META-INF discarding
assemblyMergeStrategy in assembly ~= { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}





