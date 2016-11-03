package com.wisedu.next.models

import java.util.UUID

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient

case class FeedStat(feedId: UUID, readNum: Long, likeNum: Long, unLikeNum: Long, collectNum: Long,
                    updateNum: Long, shareNum: Long, onlineNum: Long, voteNum: Long, imgNum: Long)

abstract class ConcreteFeedStats(val client: MysqlClient) {

  def getById(feedId: UUID): Future[Option[FeedStat]] = {
    val query = "SELECT * FROM feedStats WHERE feedId = '" + feedId.toString + "' limit 1 "
    client.select[FeedStat](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val readNum = row("readNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val likeNum = row("likeNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val unLikeNum = row("unLikeNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val collectNum = row("collectNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val updateNum = row("updateNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val shareNum = row("shareNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val onlineNum = row("onlineNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val voteNum = row("voteNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val imgNum = row("imgNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      FeedStat(UUID.fromString(feedId), readNum, likeNum, unLikeNum, collectNum, updateNum, shareNum, onlineNum,
        voteNum, imgNum)
    }.map(_.headOption)
  }

  def delById(feedId: UUID): Future[Result] = {
    val delSql = "delete from feedStats where feedId = '" + feedId.toString + "'"
    client.query(delSql)
  }

  def insFeedStat(feedStat: FeedStat): Future[Result] = {
    val insertSql = """INSERT INTO feedStats (feedId, readNum, likeNum, unLikeNum, collectNum, updateNum,
      shareNum, onlineNum, voteNum, imgNum) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    val ps = client.prepare(insertSql)
    ps(feedStat.feedId.toString, feedStat.readNum, feedStat.likeNum, feedStat.unLikeNum, feedStat.collectNum,
      feedStat.updateNum, feedStat.shareNum, feedStat.onlineNum, feedStat.voteNum, feedStat.imgNum)
  }

  def incFeedReadNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set readNum = readNum + 1 where feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def decFeedReadNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set readNum = readNum - 1 where feedId = ? and readNum >= 1"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def incFeedLikeNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set likeNum = likeNum + 1 where feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def decFeedLikeNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set likeNum = likeNum - 1 where feedId = ? and likeNum >= 1"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def incFeedUnLikeNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set unLikeNum = unLikeNum + 1 where feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def decFeedUnLikeNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set unLikeNum = unLikeNum - 1 where feedId = ? and unLikeNum >= 1"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def incFeedCollectNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set collectNum = collectNum + 1 where feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def decFeedCollectNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set collectNum = collectNum - 1 where feedId = ? and collectNum >= 1"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def incFeedUpdateNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set updateNum = updateNum + 1 where feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def decFeedUpdateNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set updateNum = updateNum - 1 where feedId = ? and updateNum >= 1"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def incFeedShareNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set shareNum = shareNum + 1 where feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def decFeedShareNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set shareNum = shareNum - 1 where feedId = ? and shareNum >= 1"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def incFeedOnlineNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set onlineNum = onlineNum + 1 where feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def decFeedOnlineNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set onlineNum = onlineNum - 1 where feedId = ? and onlineNum >= 1"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def incFeedVoteNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set voteNum = voteNum + 1 where feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def decFeedVoteNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set voteNum = voteNum - 1 where feedId = ? and voteNum >= 1"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def incFeedImgNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set imgNum = imgNum + 1 where feedId = ?"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def decFeedImgNum(feedId: UUID): Future[Result] = {
    val updSql = """update feedStats set imgNum = imgNum - 1 where feedId = ? and imgNum >= 1"""
    val ps = client.prepare(updSql)
    ps(feedId.toString)
  }

  def getUpdateNumById(feedId: UUID): Future[Option[Long]] = {
    val query = "select updateNum as value from feedStats where feedId = '" + feedId.toString + "'"
    client.select(query){ row =>
      val LongValue(value) = row("value").getOrElse("")
      value.toLong
    }.map(_.headOption)
  }

  def getLikeNumById(feed_id: UUID): Future[Long] = {
    val query = "select likeNum as value from feedStats where feedId = '" + feed_id.toString + "'"
    client.select(query){ row =>
      val LongValue(value) = row("value").getOrElse("")
      value.toLong
    }.map(_.head)
  }
}