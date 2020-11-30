package POC

import scala.io.Source

object Poc extends App {

  val multiLineText =
  """"Love1 good 2bad cruel4?
    |happy fun, knowledge?
    |pis po nie wiem 1 23 56-55443 ?
    |What is meant by the 'left' and the 'right'?
    |How do instant runoffs work?
    |How does direct democracy compare to representative democracy?
    |Why is the French democracy not using proportional representation for election of the assembly?
    |What are the consequences of recalling ambassadors?
    |What challenges remain for online voting?
    |N/A
    |N/A
    |How does Single Transferable Vote work?
    |N/A
    |N/A
    |Is it possible to implement an electronic voting system which is as secure as pen
    |N/A
    |home rich good love
    |bad horrible akward
    |""".stripMargin

  val pos_neg_words = Source.fromFile("src/main/scala/POC/positive_and_negative_words.txt")
    .getLines().toVector.map(_.split(","))
    .map(array => (array(0), array(1).toInt)).toMap

  val tokenization = multiLineText
    .split("\n")
    .map { line =>
      line.split("\\s+").map(word =>
        word.replaceAll("\\W", "").replaceAll("\\d", "").toLowerCase)
        .filter(_.length != 0)
        .map {
          case token if pos_neg_words.contains(token) => (token, pos_neg_words(token))
          case token => (token, 0)
        }
    }

  val calculate_score = tokenization.map { line =>
    val score = line.foldLeft(0.0) { (acc, tuple) => acc + tuple._2 }

    score / line.length
  }

  tokenization.foreach {
    token => println(token.mkString(", "))
  }

  val text_with_score = multiLineText.split("\n")
    .zip(calculate_score)
    .map(pair => (pair._1, SentimentTypeLex.fromScore(pair._2)))

  println(text_with_score.mkString("\n"))

}
