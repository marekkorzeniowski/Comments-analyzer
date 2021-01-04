package Common

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Common {

  def parseBody(body: String): String = {
    val regex = "&lt;(.+?)&gt;".r

    val processed = regex.replaceAllIn(body, "")
      .replaceAll("\\s+", " ")
      .replaceAll("&quot;", "'")
      .replaceAll("&amp;", "")

    processed
  }

  def parseTags(tags: String): String =  tags
    .replace("&lt;", "")
    .replace("&gt;", " ")
    .replaceAll("\\s+", " ")

  def parseTitle(text: String): String =
    text.replaceAll("&quot;", "'")
      .replaceAll("\\s+", " ")
      .replaceAll("&amp;", "")

  def getCurrentTimeStamp: String = {
    val format = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss.S")

    LocalDateTime.now().format(format)
  }

  def getTimeStampFromString(s: String): Timestamp = {
    val formatted  = s.replace("T", " ")
    Timestamp.valueOf(formatted)
  }

}
