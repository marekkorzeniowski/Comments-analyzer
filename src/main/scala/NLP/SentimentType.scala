package NLP

sealed trait SentimentType

object SentimentType {
  // scores should be between -1 and 1
  def fromScore(score: Float): SentimentType =
    if (score >= 0.3) VERY_POSITIVE
    else if (score >= 0.05) POSITIVE
    else if (score >= -0.05) NEUTRAL
    else if (score >= -0.3) NEGATIVE
    else if (score >= -1) VERY_NEGATIVE
    else NOT_UNDERSTOOD
}

case object VERY_NEGATIVE extends SentimentType
case object NEGATIVE extends SentimentType
case object NEUTRAL extends SentimentType
case object POSITIVE extends SentimentType
case object VERY_POSITIVE extends SentimentType
case object NOT_UNDERSTOOD extends SentimentType
