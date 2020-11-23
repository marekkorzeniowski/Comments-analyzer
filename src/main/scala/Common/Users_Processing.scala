package Common

import Spark_App.Consumer.spark
import com.databricks.spark.xml.XmlDataFrameReader
import org.apache.spark.sql.functions.col

case class UserID(id:Long, name: Option[String], location : Option[String])

case class User(name: Option[String], location : Option[String])

object Users_Processing {

  val users_source = "/home/marek/Repos/Comments-analyzer/src/main/resources/data/Users.xml"

 import spark.implicits._

  val usersDS = spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "row")
    .xml(users_source).
    select(col("_Id").as("id"),
      col("_DisplayName").as("name"),
      col("_Location").as("location"))
    .as[UserID]
    .map(user => (user.id, User(user.name, user.location)))

  val processed_users = "/home/marek/Repos/Comments-analyzer/src/main/resources/data/users_parquet"

  usersDS.write.parquet(processed_users)
}
