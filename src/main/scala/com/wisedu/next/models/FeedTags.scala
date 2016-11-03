package com.wisedu.next.models

import java.util.UUID

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient

case class FeedTag(feedId: UUID, tagId: String, tagRank: Int, errorValue: Int)

abstract class ConcreteFeedTags(val client: MysqlClient) {

  def getById(feedId: UUID, tagId: String): Future[Option[FeedTag]] = {
    val query = "SELECT * FROM feedTags WHERE feedId = '" + feedId.toString +
      "' and tagId = '" + tagId + "' limit 1 "
    client.select[FeedTag](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tagId = row("tagId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(tagRank) = row("tagRank").getOrElse("")
      val IntValue(errorValue) = row("errorValue").getOrElse("")
      FeedTag(UUID.fromString(feedId), tagId, tagRank, errorValue)
    }.map(_.headOption)
  }

  def delById(feedId: UUID, tagId: String): Future[Result] = {
    val delSql = "delete from feedTags where feedId = '" + feedId.toString + "' and tagId = '" + tagId.toString + "'"
    client.query(delSql)
  }

  def insFeedTag(feedTag: FeedTag): Future[Result] = {
    val insertSql = """INSERT INTO feedTags (feedId, tagId, tagRank, errorValue) VALUES (?, ?, ?, ?)"""
    val ps = client.prepare(insertSql)
    ps(feedTag.feedId.toString, feedTag.tagId, feedTag.tagRank, feedTag.errorValue)
  }

  def updFeedTag(feedTag: FeedTag): Future[Result] = {
    val updSql = """update feedTags set tagRank = ?, errorValue = ? where tagId = ? and feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedTag.tagRank, feedTag.errorValue, feedTag.tagId, feedTag.feedId.toString)
  }

  def getFeedIdByTagId(tagId: String): Future[Seq[String]] = {
    val query = "SELECT feedId FROM feedTags WHERE tagId = '" + tagId + "'"
    client.select[String](query) { row =>
      val StringValue(feedId) = row("feedId").getOrElse("")
      feedId
    }
  }

  def getFeedTagsByFeedId(feedId: UUID): Future[Seq[FeedTag]] = {
    val query = "SELECT * FROM feedTags  WHERE feedId = '" + feedId.toString +
      "' and feedId = '" + feedId.toString + "'"
    client.select[FeedTag](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tagId = row("tagId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(tagRank) = row("tagRank").getOrElse("")
      val IntValue(errorValue) = row("errorValue").getOrElse("")
      FeedTag(UUID.fromString(feedId), tagId, tagRank, errorValue)
    }
  }

  def delFeedTagsByFeedId(feedId: UUID): Future[Result] = {
    val delSql = "delete FROM feedTags WHERE feedId = '" + feedId.toString + "'"
    client.query(delSql)
  }

  def getFeedIdByTagIds(tagIds: String, limits: Int, offset: Int): Future[Seq[String]] = {
    val query = "SELECT feedId FROM feedTags WHERE tagId in ('" + tagIds.replaceAll(",", "','") + "') limit " + offset + ", " + limits
    client.select[String](query) { row =>
      val StringValue(feedId) = row("feedId").getOrElse("")
      feedId
    }
  }
}