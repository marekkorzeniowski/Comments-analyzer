package Common

import java.time.{LocalDate, LocalTime}

case class Comment(
                  postId: Int,
                  score: Int,
                  text: String,
                  creationDate: LocalDate,
                  creationTime: LocalTime
                  )



/*
Available attributes in the xml comment:
PostId
Score
Text
CreationDate
UserId *
UserDisplayName *
 */