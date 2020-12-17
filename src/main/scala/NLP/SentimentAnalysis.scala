package NLP

import Main.Consumer.spark
import org.apache.spark.broadcast.Broadcast

import scala.io.Source

object SentimentAnalysis {

  def readWords(path: String): Broadcast[Map[String, Int]] = {
    val mapOfWords = Source.fromFile(path)
      .getLines().toVector
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
