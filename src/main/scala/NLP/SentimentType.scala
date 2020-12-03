package NLP

sealed trait SentimentType

object SentimentType {
  // scores should be between -1 and 1
  def fromScore(score: Double): SentimentType =
    if (score >= 0.6) VERY_POSITIVE
    else if (score >= 0.2) POSITIVE
    else if (score >= -0.2) NEUTRAL
    else if (score >= -0.6) NEGATIVE
    else VERY_NEGATIVE
}

case object VERY_NEGATIVE extends SentimentType
case object NEGATIVE extends SentimentType
case object NEUTRAL extends SentimentType
case object POSITIVE extends SentimentType
case object VERY_POSITIVE extends SentimentType
