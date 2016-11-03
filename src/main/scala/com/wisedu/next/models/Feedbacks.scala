package com.wisedu.next.models

import com.twitter.util.Future
import com.websudos.phantom.dsl._

case class Feedback(userId: UUID, cTime: DateTime, content: String, contact: String)

class Feedbacks extends CassandraTable[Feedbacks, Feedback] {
  object userId extends UUIDColumn(this) with PartitionKey[UUID]
  object cTime extends DateTimeColumn(this) with PrimaryKey[DateTime]
  object content extends StringColumn(this)
  object contact extends StringColumn(this)

  def fromRow(row: Row): Feedback = {
    Feedback(
      userId(row), cTime(row), content(row), contact(row)
    )
  }
}

abstract class ConcreteFeedbacks extends Feedbacks with RootConnector {

  def insFeedback(feedback: Feedback): Future[ResultSet] = {
    insert.value(_.userId, feedback.userId)
      .value(_.cTime, feedback.cTime)
      .value(_.content, feedback.content)
      .value(_.contact, feedback.contact).execute()
  }

}