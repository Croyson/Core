package com.wisedu.next.models

/**
  * Version: 1.0
  * Author: pattywgm 
  * Time: 16/5/22 下午4:23
  * Desc:
  */

import java.util.UUID

import com.twitter.finagle.client
import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._
import org.joda.time.DateTime

case class FeedShare(shareId: UUID, feedId: UUID, userId: UUID, cTime: DateTime, shareType: Int, shareUrl: String)

class FeedShares extends CassandraTable[FeedShares, FeedShare] {

  object shareId extends UUIDColumn(this) with PrimaryKey[UUID]

  object feedId extends UUIDColumn(this)

  object userId extends UUIDColumn(this)

  object cTime extends DateTimeColumn(this)

  object shareType extends IntColumn(this)

  object shareUrl extends StringColumn(this)

  def fromRow(row: Row): FeedShare = {
    FeedShare(
      shareId(row), feedId(row), userId(row), cTime(row), shareType(row), shareUrl(row))
  }
}

abstract class ConcreteFeedShares extends FeedShares with RootConnector {

  def insFeedShare(feedShare: FeedShare): Future[ResultSet] = {
    insert.value(_.shareId, feedShare.shareId)
      .value(_.userId, feedShare.userId)
      .value(_.feedId, feedShare.feedId)
      .value(_.cTime, feedShare.cTime)
      .value(_.shareType, feedShare.shareType)
      .value(_.shareUrl, feedShare.shareUrl)
      .execute()
  }

  def delFeedShare(shareId: UUID): Future[ResultSet] = {
    delete.where(_.shareId eqs shareId).execute()
  }

}

