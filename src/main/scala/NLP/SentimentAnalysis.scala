package NLP

import Main.Consumer.spark
import org.apache.spark.broadcast.Broadcast

object SentimentAnalysis {

  def readWords(path: String): Broadcast[Map[String, Int]] = {

    val rddOfWords = spark.sparkContext.textFile(path)

    val mapOfWords = rddOfWords.collect().map(_.split(","))
      .map(array => (array(0), array(1).toInt)).toMap

    spark.sparkContext.broadcast(mapOfWords)
  }

  def tokenizer(text: String, lexicon: Broadcast[Map[String, Int]]): Array[(String, Int)] = {
    text.split("\\s+")
      .map(word =>
      word.replaceAll("\\W", "").replaceAll("\\d", "").toLowerCase)
      .filter(_.length != 0)
      .map {
        case token => (token, lexicon.value.getOrElse(token, 0))

      }
  }

  def calculateAvgScore(tokens: Array[(String, Int)]) = {
    val score = tokens.foldLeft(0.0) { (acc, tuple) => acc + tuple._2 }

    score / tokens.length
  }
}
