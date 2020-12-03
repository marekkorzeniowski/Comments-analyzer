package Auxiliary

import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source

object ExtractLexicon extends App {

  val positive_words =
    Source.fromFile("src/main/scala/POC/positive_words.txt").getLines().toVector
      .map(word => (word, 1))

  val negative_words = Source.fromFile("src/main/scala/POC/negative_words.txt").getLines().toVector
    .map(word => (word, -1))

  val concat = (positive_words ++ negative_words).sortBy(_._1)

  def writeFile(filename: String, lines: Seq[(String, Int)]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      val parsedLine = line.toString().replaceAll("\\(", "").replaceAll("\\)", "\n")
      bw.write(parsedLine)
    }
    bw.close()
  }

  writeFile("src/main/scala/POC/positive_and_negative_words.txt", concat)

}
