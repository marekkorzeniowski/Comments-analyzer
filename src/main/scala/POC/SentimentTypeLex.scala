package POC

sealed trait SentimentTypeLex

object SentimentTypeLex {
  // scores should be between -1 and 1
  def fromScore(score: Double): SentimentTypeLex =
    if (score >= 0.6) VERY_POSITIVE
    else if (score >= 0.2) POSITIVE
    else if (score >= -0.2) NEUTRAL
    else if (score >= -0.6) NEGATIVE
    else VERY_NEGATIVE
}

case object VERY_NEGATIVE extends SentimentTypeLex
case object NEGATIVE extends SentimentTypeLex
case object NEUTRAL extends SentimentTypeLex
case object POSITIVE extends SentimentTypeLex
case object VERY_POSITIVE extends SentimentTypeLex
