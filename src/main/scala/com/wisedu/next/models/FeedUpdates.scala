package com.wisedu.next.models

import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class FeedUpdate(updateId: UUID, feedId: UUID, userId: UUID, content: String, cTime: DateTime, likeNum: Long,
                      unLikeNum: Long, pUpdateId: UUID, updateLevel: Int, imgUrls: String, subUpdateNum: Long,
                      isAnonymous: Int, updateType: Int, threshHold: Int, fuzzyImgs: String, isDelete: Int)

abstract class ConcreteFeedUpdates(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getById(updateId: UUID): Future[Option[FeedUpdate]] = {
    val query = "SELECT * FROM feedUpdates WHERE isDelete = 0 and  updateId = '" + updateId.toString + "' limit 1 "
    client.select[FeedUpdate](query) { row =>
      val updateId = row("updateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val content = row("content").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val imgUrls = row("imgUrls").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("")
      val LongValue(likeNum) = row("likeNum").getOrElse("")
      val LongValue(unLikeNum) = row("unLikeNum").getOrElse("")
      val pUpdateId = row("pUpdateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(updateLevel) = row("updateLevel").getOrElse("")
      val LongValue(subUpdateNum) = row("subUpdateNum").getOrElse("")
      val IntValue(isAnonymous) = row("isAnonymous").getOrElse("")
      val IntValue(updateType) = row("updateType").getOrElse("")
      val IntValue(threshHold) = row("threshHold").getOrElse("")
      val fuzzyImgs = row("fuzzyImgs").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val IntValue(isDelete) = row("isDelete").getOrElse("")
      FeedUpdate(UUID.fromString(updateId), UUID.fromString(feedId), UUID.fromString(userId), content,
        new DateTime(cTime), likeNum, unLikeNum, UUID.fromString(pUpdateId), updateLevel, imgUrls, subUpdateNum,isAnonymous,updateType,threshHold
        ,fuzzyImgs,isDelete)
    }.map(_.headOption)
  }

  def delById(updateId: UUID): Future[Result] = {
    val delSql = "delete from feedUpdates where updateId = '" + updateId.toString + "'"
    client.query(delSql)
  }

  def delLogicById(updateId: UUID,userId:String): Future[Result] = {
    val delSql = "update feedUpdates set isDelete = 1 where updateId = '" + updateId.toString + "' and userId = '"+userId+"'"
    client.query(delSql)
  }


  def insFeedUpdate(feedUpdate: FeedUpdate): Future[Result] = {
    val insertSql =
      """INSERT INTO feedUpdates (updateId, feedId, userId, content, cTime, likeNum, unLikeNum,
        pUpdateId, updateLevel, imgUrls, subUpdateNum,isAnonymous, updateType, threshHold,fuzzyImgs)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)"""
    val ps = client.prepare(insertSql)
    ps(feedUpdate.updateId.toString, feedUpdate.feedId.toString, feedUpdate.userId.toString, feedUpdate.content,
      feedUpdate.cTime.toDate, feedUpdate.likeNum, feedUpdate.unLikeNum, feedUpdate.pUpdateId.toString,
      feedUpdate.updateLevel, feedUpdate.imgUrls, feedUpdate.subUpdateNum, feedUpdate.isAnonymous,
      feedUpdate.updateType, feedUpdate.threshHold, feedUpdate.fuzzyImgs)
  }

  def incSubUpdateNum(updateId: UUID): Future[Result] = {
    val updSql = """update feedUpdates set subUpdateNum = subUpdateNum + 1 where updateId = ?"""
    val ps = client.prepare(updSql)
    ps(updateId.toString)
  }

  def decSubUpdateNum(updateId: UUID): Future[Result] = {
    val updSql = """update feedUpdates set subUpdateNum = subUpdateNum - 1 where updateId = ? and subUpdateNum >= 1"""
    val ps = client.prepare(updSql)
    ps(updateId.toString)
  }

  def incFeedUpdateLikeNum(updateId: UUID): Future[Result] = {
    val updSql = """update feedUpdates set likeNum = likeNum + 1 where updateId = ?"""
    val ps = client.prepare(updSql)
    ps(updateId.toString)
  }

  def decFeedUpdateLikeNum(updateId: UUID): Future[Result] = {
    val updSql = """update feedUpdates set likeNum = likeNum - 1 where updateId = ? and likeNum >= 1"""
    val ps = client.prepare(updSql)
    ps(updateId.toString)
  }

  def incFeedUpdateUnLikeNum(updateId: UUID): Future[Result] = {
    val updSql = """update feedUpdates set UnLikeNum = UnLikeNum + 1 where updateId = ?"""
    val ps = client.prepare(updSql)
    ps(updateId.toString)
  }

  def decFeedUpdateUnLikeNum(updateId: UUID): Future[Result] = {
    val updSql = """update feedUpdates set UnLikeNum = UnLikeNum - 1 where updateId = ? and UnLikeNum >= 1"""
    val ps = client.prepare(updSql)
    ps(updateId.toString)
  }

  def getHotUpdatesByFeedId(feed_id: UUID, limits: Int): Future[Seq[FeedUpdate]] = {
    val query = "select * from feedUpdates where  isDelete = 0 and  feedId = '" + feed_id.toString + "' and updateLevel = 0 and likeNum > 5 ORDER BY likeNum DESC, cTime DESC limit " + limits
    client.select[FeedUpdate](query) { row =>
      val updateId = row("updateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val content = row("content").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val imgUrls = row("imgUrls").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("")
      val LongValue(likeNum) = row("likeNum").getOrElse("")
      val LongValue(unLikeNum) = row("unLikeNum").getOrElse("")
      val pUpdateId = row("pUpdateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(updateLevel) = row("updateLevel").getOrElse("")
      val LongValue(subUpdateNum) = row("subUpdateNum").getOrElse("")
      val IntValue(isAnonymous) = row("isAnonymous").getOrElse("")
      val IntValue(updateType) = row("updateType").getOrElse("")
      val IntValue(threshHold) = row("threshHold").getOrElse("")
       val fuzzyImgs = row("fuzzyImgs").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val IntValue(isDelete) = row("isDelete").getOrElse("")
      FeedUpdate(UUID.fromString(updateId), UUID.fromString(feedId), UUID.fromString(userId), content,
        new DateTime(cTime), likeNum, unLikeNum, UUID.fromString(pUpdateId), updateLevel, imgUrls, subUpdateNum,isAnonymous,updateType,threshHold
        ,fuzzyImgs,isDelete)
    }
  }

  def collUpdatesById(id: String, id_type: String, limits: Int, offset: Int): Future[Seq[FeedUpdate]] = {
    var query = "select * from feedUpdates where  isDelete = 0 and "

    if (id_type.equals("feedId")) {
      query += "feedId = '" + id + "' and updateLevel = 0"
    }
    if (id_type.equals("updateId")) {
      query += "pUpdateId = '" + id + "' and updateLevel = 1"
    }
    query += " ORDER BY cTime DESC limit " + offset + ", " + limits

    client.select[FeedUpdate](query) { row =>
      val updateId = row("updateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val content = row("content").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val imgUrls = row("imgUrls").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("")
      val LongValue(likeNum) = row("likeNum").getOrElse("")
      val LongValue(unLikeNum) = row("unLikeNum").getOrElse("")
      val pUpdateId = row("pUpdateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymous) = row("isAnonymous").getOrElse("")
      val IntValue(updateLevel) = row("updateLevel").getOrElse("")
      val LongValue(subUpdateNum) = row("subUpdateNum").getOrElse("")
      val IntValue(updateType) = row("updateType").getOrElse("")
      val IntValue(threshHold) = row("threshHold").getOrElse("")
      val fuzzyImgs = row("fuzzyImgs").map {
        case StringValue(str) => str
        case _ => ""
      }.get

     val IntValue(isDelete) = row("isDelete").getOrElse("")
      FeedUpdate(UUID.fromString(updateId), UUID.fromString(feedId), UUID.fromString(userId), content,
        new DateTime(cTime), likeNum, unLikeNum, UUID.fromString(pUpdateId), updateLevel, imgUrls, subUpdateNum,isAnonymous,updateType,threshHold
        ,fuzzyImgs,isDelete)
     }
  }

  def collUpdatesByIdWithUserIds(feedId: String, userIds: String, limits: Int, offset: Int): Future[Seq[FeedUpdate]] = {
    var query =
      """select * from
                  (
                  select *,CONCAT("Top_",date_format(cTime,'%Y-%m-%d %H:%i:%s')) as sortNo from feedUpdates where  isDelete = 0 and  feedId = '""" + feedId +
        """' and updateLevel = 0 AND  isAnonymous = 0 and  userId   in ('""" + userIds.replaceAll(",", "','") +
        """')
                  union
                  select *,date_format(cTime,'%Y-%m-%d %H:%i:%s')  as sortNo from feedUpdates where  isDelete = 0 and  feedId = '""" + feedId +
        """' and updateLevel = 0 AND (isAnonymous = 1 or userId NOT in ('""" + userIds.replaceAll(",", "','") +
        """'))
                  ) t order by sortNo desc
        """
    query += "   limit " + offset + ", " + limits

    client.select[FeedUpdate](query) { row =>
      val updateId = row("updateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val userId = row("userId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val content = row("content").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val imgUrls = row("imgUrls").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("")
      val LongValue(likeNum) = row("likeNum").getOrElse("")
      val LongValue(unLikeNum) = row("unLikeNum").getOrElse("")
      val pUpdateId = row("pUpdateId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isAnonymous) = row("isAnonymous").getOrElse("")
      val IntValue(updateLevel) = row("updateLevel").getOrElse("")
      val LongValue(subUpdateNum) = row("subUpdateNum").getOrElse("")
      val IntValue(updateType) = row("updateType").getOrElse("")
      val IntValue(threshHold) = row("threshHold").getOrElse("")
     val fuzzyImgs = row("fuzzyImgs").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(isDelete) = row("isDelete").getOrElse("")
      FeedUpdate(UUID.fromString(updateId), UUID.fromString(feedId), UUID.fromString(userId), content,
        new DateTime(cTime), likeNum, unLikeNum, UUID.fromString(pUpdateId), updateLevel, imgUrls, subUpdateNum,isAnonymous,updateType,threshHold
        ,fuzzyImgs,isDelete)
        }
  }


  def getSubUpdateNumById(id: UUID): Future[Long] = {
    val query = "select subUpdateNum as value from feedUpdates where isDelete = 0 and  updateId = '" + id.toString + "'"
    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toLong
    }.map(_.head)
  }

  def getUpdateLikeNumById(id: String): Future[Long] = {
    val query = "select likeNum as value from feedUpdates where isDelete = 0 and updateId = '" + id + "'"
    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toLong
    }.map(_.head)
  }
}