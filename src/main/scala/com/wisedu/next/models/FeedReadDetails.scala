package com.wisedu.next.models

import com.twitter.util.Future
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._

case class FeedReadDetail(id: UUID, feedId: UUID, userId: String, readType: Int, readTime: DateTime, srcValue: String, pushValue: String, ipAddr: String)

class FeedReadDetails extends CassandraTable[FeedReadDetails, FeedReadDetail] {
  object id extends UUIDColumn(this) with PrimaryKey[UUID]
  object feedId extends UUIDColumn(this)
  object userId extends StringColumn(this)
  object readType extends IntColumn(this)
  object readTime extends DateTimeColumn(this)
  object srcValue extends StringColumn(this)
  object pushValue extends StringColumn(this)
  object ipAddr extends StringColumn(this)

  def fromRow(row: Row): FeedReadDetail = {
    FeedReadDetail(
      id(row), feedId(row), userId(row), readType(row), readTime(row), srcValue(row), pushValue(row), ipAddr(row))
  }
}

abstract class ConcreteFeedReadDetails extends FeedReadDetails with RootConnector {

  def insFeedReadDetails(feedReadDetail: FeedReadDetail): Future[ResultSet] = {
    insert.value(_.id, feedReadDetail.id).value(_.userId, feedReadDetail.userId)
      .value(_.feedId, feedReadDetail.feedId)
      .value(_.readType, feedReadDetail.readType)
      .value(_.readTime, feedReadDetail.readTime)
      .value(_.srcValue, feedReadDetail.srcValue)
      .value(_.pushValue, feedReadDetail.pushValue)
      .value(_.ipAddr, feedReadDetail.ipAddr)
      .execute()
  }

}
