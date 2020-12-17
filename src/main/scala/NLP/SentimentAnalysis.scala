package NLP

import Main.Consumer.spark
import org.apache.spark.broadcast.Broadcast

object SentimentAnalysis {

  def readWords(path: String): Broadcast[Map[String, Int]] = {
    val rddOfWords = spark.sparkContext.textFile(path)

    val mapOfWords = rddOfWords.collect()
      .map(line => (line, 1)).toMap

    spark.sparkContext.broadcast(mapOfWords)
  }

  def tokenizer(text: String, lexicon: Broadcast[Map[String, Int]]): Array[(String, Int)] = {
    text.split("\\s+")
      .map(word =>
      word.replaceAll("\\W", "")
        .replaceAll("\\d", "")
        .toLowerCase)
      .filter(_.nonEmpty)
      .map(token => (token, lexicon.value.getOrElse(token, 0)))
  }

  def getScore(tokens: Array[(String, Int)]): Int = {
    tokens.foldLeft(0) { (acc, tuple) => acc + tuple._2 }
  }
}
