package Common

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

  def parseTitle(text: String) =
    text.replaceAll("&quot;", "'")
      .replaceAll("\\s+", " ")
      .replaceAll("&amp;", "")


}
