package com.wisedu.next.models

import java.text.SimpleDateFormat
import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.redis.Client
import com.twitter.util.Future
import com.websudos.phantom.dsl.{Row, _}
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.{DateTime, DateTimeZone}

case class MessageInfoC(messageId: UUID, content: String)

case class MessageInfo(messageId: UUID, feedId: String, messageType: Int, content: String,
                       cTime: DateTime, cUserId: String, collectNum: Long, updateNum: Long, updateNumD: Long,
                       forwardNum: Long, forwardNumD: Long, likeNum: Long, unLikeNum: Long, readNum: Long, updateType: Int,
                       threshHold: Int, isAnonymous: Int, imgUrls: String, fuzzyImgs: String, updateLevel: Int, isDelete: Int, groupId: String,
                       mImgs: String, isTop: Int, isRecommend: Int, replyMsgId: String, replyUserId: String, linkTitle: String, linkImg: String, linkUrl: String)

case class MsgRelation(referenceUserId: String, referenceMsgId: String, referencedUserId: String, referencedMsgId: String,
                       opType: Int, cTime: DateTime, pageIndex: Int)


case class UserMsgNotice(msgId: UUID, cUserId: String, cTime: DateTime, content: String, imgUrls: String,
                         circleId: String, opUserId: String, opTime: DateTime, opMsgId: String,
                         opContent: String, opImgUrls: String, opType: String, mainMsgId: UUID)


class MessageInfos extends CassandraTable[MessageInfos, MessageInfoC] {

  object messageId extends UUIDColumn(this) with PrimaryKey[UUID]

  object content extends StringColumn(this)


  def fromRow(row: Row): MessageInfoC = {
    MessageInfoC(messageId(row), content(row))
  }
}

abstract class ConcreteMessageInfos(val client: MysqlClient, val redisClient: Client) extends MessageInfos with RootConnector {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def toMessageInfoC(messageInfo: MessageInfo): MessageInfoC = {
    MessageInfoC(messageInfo.messageId, messageInfo.content)
  }

  def updateMsgRecommend(msgId: String, status: Int): Future[Result] = {
    val sql = "update messageInfos set isRecommend = ? where messageId = ?"
    client.prepare(sql)(status, msgId)
  }


  def insMessageInfo(messageInfo: MessageInfo): Future[Result] = {
    insert.value(_.messageId, messageInfo.messageId)
      .value(_.content, messageInfo.content).execute().flatMap {
      rst => {
        val sql =
          """insert into messageInfos(messageId,feedId,messageType,cTime,cUserId,collectNum,updateNum,updateNumD,forwardNum,forwardNumD,likeNum,unLikeNum,readNum,
                    updateType,threshHold,isAnonymous,imgUrls,fuzzyImgs,updateLevel,isDelete,groupId,mImgs,isTop,isRecommend,replyMsgId,replyUserId, linkTitle, linkImg, linkUrl)
                    VALUES(?,?,?,now(),?,0,0,0,0,0,0,0,0,?,?,?,?,?,?,0,?,?,?,0,?,?,?,?,?)"""
        client.prepare(sql)(messageInfo.messageId.toString, messageInfo.feedId.toString, messageInfo.messageType, messageInfo.cUserId,
          messageInfo.updateType, messageInfo.threshHold, messageInfo.isAnonymous, messageInfo.imgUrls, messageInfo.fuzzyImgs,
          messageInfo.updateLevel, messageInfo.groupId, messageInfo.mImgs, messageInfo.isTop, messageInfo.replyMsgId, messageInfo.replyUserId,
          messageInfo.linkTitle, messageInfo.linkImg, messageInfo.linkUrl)

      }
    }
  }

  def insMsgRelation(msgRelation: MsgRelation): Future[Result] = {
    val sql =
      """
       insert INTO msgRelations(referenceUserId,referenceMsgId,referencedUserId,referencedMsgId,opType,cTime,pageIndex)
       values(?,?,?,?,?,now(),?)
      """
    client.prepare(sql)(msgRelation.referenceUserId, msgRelation.referenceMsgId, msgRelation.referencedUserId,
      msgRelation.referencedMsgId, msgRelation.opType, msgRelation.pageIndex)
  }

  def delMessageInfo(messageId: UUID): Future[Result] = {
    val sql = "update messageInfos set isDelete = 1  where messageId = ?"
    client.prepare(sql)(messageId.toString)
  }

  def delMessageInfoByAdmin(messageId: UUID): Future[Result] = {
    val sql = "update messageInfos set isDelete = 2  where messageId = ?"
    client.prepare(sql)(messageId.toString)
  }

  def changeMsgTop(messageId: UUID, topStatus: Int): Future[Result] = {
    val updSql = """update  messageInfos set isTop = ? where messageId = ?"""
    val ps = client.prepare(updSql)
    ps(topStatus, messageId.toString)
  }

  def incMsgReadNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set readNum = readNum + 1 where messageId = ?"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }

  def incMsgCollectNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set collectNum = collectNum + 1 where messageId = ?"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }

  def decMsgCollectNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set collectNum = collectNum - 1 where messageId = ? and collectNum > 0"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }

  def incMsgUpdateNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set updateNum = updateNum + 1,updateNumD = updateNumD + 1 where messageId = ?"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }

  def decMsgUpdateNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set updateNum = updateNum - 1 where messageId = ? and updateNum > 0"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }

  def incMsgForwardNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set forwardNum = forwardNum + 1,forwardNumD = forwardNumD + 1 where messageId = ?"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }

  def decMsgForwardNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set forwardNum = forwardNum - 1 where messageId = ? and forwardNum > 0"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }


  def incMsgLikeNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set likeNum = likeNum + 1 where messageId = ?"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }

  def decMsgLikeNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set likeNum = likeNum - 1 where messageId = ? and likeNum > 0"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }

  def incMsgUnLikeNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set unLikeNum = unLikeNum + 1 where messageId = ?"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }

  def decMsgUnLikeNum(messageId: UUID): Future[Result] = {
    val updSql = """update  messageInfos set unLikeNum = unLikeNum - 1 where messageId = ? and unLikeNum > 0"""
    val ps = client.prepare(updSql)
    ps(messageId.toString)
  }

  def getMsgById(messageId: UUID): Future[Option[MessageInfo]] = {
    select.where(_.messageId eqs messageId).get().map {
      case Some(msg) => msg.content
      case None => ""
    }.flatMap {
      content => val sql =
        """
       select * from messageInfos where messageId = ? and isDelete != 1  limit 1
        """
        client.prepare(sql).select[MessageInfo](messageId.toString){row =>
        val messageId = row("messageId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val messageType = row("messageType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
        val cUserId = row("cUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val collectNum = row("collectNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNum = row("updateNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNumD = row("updateNumD").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNum = row("forwardNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNumD = row("forwardNum").map {
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
        val readNum = row("readNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateType = row("updateType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val threshHold = row("threshHold").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isAnonymous = row("isAnonymous").map {
          case IntValue(str) => str
          case _ => 0
        }.get

         val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val fuzzyImgs = row("fuzzyImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val updateLevel = row("updateLevel").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val isDelete = row("isDelete").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val mImgs = row("mImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val isTop = row("isTop").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isRecommend = row("isRecommend").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val replyMsgId = row("replyMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val replyUserId = row("replyUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val linkTitle = row("linkTitle").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val linkImg = row("linkImg").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val linkUrl = row("linkUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        MessageInfo(UUID.fromString(messageId),feedId,messageType,content, new DateTime(cTime),cUserId,collectNum,updateNum,updateNumD,
        forwardNum,forwardNumD,likeNum,unLikeNum,readNum,updateType,threshHold,isAnonymous,imgUrls,fuzzyImgs,
        updateLevel,isDelete,groupId,mImgs,isTop,isRecommend,replyMsgId,replyUserId,linkTitle,linkImg,linkUrl)
      }.map(_.headOption)
    }
  }

  def getMsgByIds(messageIds: String, orderInfo: String): Future[Seq[MessageInfo]] = {
    var sql = " select * from messageInfos where  isDelete = 0 "
    if (messageIds.nonEmpty) {
      sql += "and messageId in ('" + messageIds.replaceAll(",", "','") + "')"
    }
    if (orderInfo.nonEmpty) {
      sql += " order by " + orderInfo + ",cTime desc"
    } else {
      sql += " order by cTime desc "
    }
    client.prepare(sql).select[MessageInfo](messageId.toString){row =>
        val messageId = row("messageId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val messageType = row("messageType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
        val cUserId = row("cUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val collectNum = row("collectNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNum = row("updateNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNumD = row("updateNumD").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNum = row("forwardNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNumD = row("forwardNum").map {
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
        val readNum = row("readNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateType = row("updateType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val threshHold = row("threshHold").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isAnonymous = row("isAnonymous").map {
          case IntValue(str) => str
          case _ => 0
        }.get

         val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val fuzzyImgs = row("fuzzyImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val updateLevel = row("updateLevel").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val isDelete = row("isDelete").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val mImgs = row("mImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val isTop = row("isTop").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val isRecommend = row("isRecommend").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val replyMsgId = row("replyMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val replyUserId = row("replyUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val linkTitle = row("linkTitle").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val linkImg = row("linkImg").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val linkUrl = row("linkUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        MessageInfo(UUID.fromString(messageId),feedId,messageType,"", new DateTime(cTime),cUserId,collectNum,updateNum,updateNumD,
        forwardNum,forwardNumD,likeNum,unLikeNum,readNum,updateType,threshHold,isAnonymous,imgUrls,fuzzyImgs,
        updateLevel,isDelete,groupId,mImgs,isTop,isRecommend,replyMsgId,replyUserId,linkTitle,linkImg,linkUrl)
      }.flatMap {
      msgs => Future.collect(msgs.map {
        msg => select.where(_.messageId eqs msg.messageId).get().map {
          case Some(t) => MessageInfo(msg.messageId, msg.feedId, msg.messageType, t.content, msg.cTime, msg.cUserId,
            msg.collectNum, msg.updateNum, msg.updateNumD, msg.forwardNum, msg.forwardNumD, msg.likeNum, msg.unLikeNum,
            msg.readNum, msg.updateType, msg.threshHold, msg.isAnonymous, msg.imgUrls, msg.fuzzyImgs,
            msg.updateLevel, msg.isDelete, msg.groupId, msg.mImgs, msg.isTop, msg.isRecommend, msg.replyMsgId,
            msg.replyUserId, msg.linkTitle, msg.linkImg, msg.linkUrl)
          case None => msg
        }
      })
    }

  }

  def getUserMsgs(userId: String, isDelete: String, isAnonymous: String, limits: Int, offset: Int): Future[Seq[MessageInfo]] = {
    var sql = " select * from messageInfos where 1=1 "
    if (isDelete.nonEmpty) {
      sql += " and isDelete in ('" + isDelete.replaceAll(",", "','") + "')"
    } else {
      sql += " and isDelete = 0 "
    }
    if (isAnonymous.nonEmpty) {
      sql += " and isAnonymous = " + isAnonymous
    }
    sql += " and messageType in (0,2) and cUserId = '" + userId + "' order by cTime desc limit " + offset + "," + limits
    client.prepare(sql).select[MessageInfo](messageId.toString){row =>
        val messageId = row("messageId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val messageType = row("messageType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
        val cUserId = row("cUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val collectNum = row("collectNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNum = row("updateNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNumD = row("updateNumD").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNum = row("forwardNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNumD = row("forwardNum").map {
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
        val readNum = row("readNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateType = row("updateType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val threshHold = row("threshHold").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isAnonymous = row("isAnonymous").map {
          case IntValue(str) => str
          case _ => 0
        }.get

         val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val fuzzyImgs = row("fuzzyImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val updateLevel = row("updateLevel").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val isDelete = row("isDelete").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val mImgs = row("mImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val isTop = row("isTop").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val isRecommend = row("isRecommend").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val replyMsgId = row("replyMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val replyUserId = row("replyUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val linkTitle = row("linkTitle").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val linkImg = row("linkImg").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val linkUrl = row("linkUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        MessageInfo(UUID.fromString(messageId),feedId,messageType,"", new DateTime(cTime),cUserId,collectNum,updateNum,updateNumD,
        forwardNum,forwardNumD,likeNum,unLikeNum,readNum,updateType,threshHold,isAnonymous,imgUrls,fuzzyImgs,
        updateLevel,isDelete,groupId,mImgs,isTop,isRecommend,replyMsgId,replyUserId,linkTitle,linkImg,linkUrl)
      }.flatMap {
      msgs => Future.collect(msgs.map {
        msg => select.where(_.messageId eqs msg.messageId).get().map {
          case Some(t) => MessageInfo(msg.messageId, msg.feedId, msg.messageType, t.content, msg.cTime, msg.cUserId,
            msg.collectNum, msg.updateNum, msg.updateNumD, msg.forwardNum, msg.forwardNumD, msg.likeNum, msg.unLikeNum,
            msg.readNum, msg.updateType, msg.threshHold, msg.isAnonymous, msg.imgUrls, msg.fuzzyImgs,
            msg.updateLevel, msg.isDelete, msg.groupId, msg.mImgs, msg.isTop, msg.isRecommend, msg.replyMsgId,
            msg.replyUserId, msg.linkTitle, msg.linkImg, msg.linkUrl)
          case None => msg
        }
      })
    }
  }


  def getMsgS(circleIds: String, feedId: String, messageId: String, updateLevel: String, messageType: String, excludeMessageId: String,
              isRecommend: String, orderInfo: String, limits: Int, offset: Int): Future[Seq[MessageInfo]] = {
    var sql = " select * from messageInfos where  isDelete = 0 "

    if (circleIds.nonEmpty) {
      if (circleIds.split(",").length == 1) {
        sql += " and groupId = '" + circleIds + "'"
      } else {
        sql += " and groupId  in ('" + circleIds.replaceAll(",", "','") + "')"
      }
    }

    if (feedId.nonEmpty) {
      //咨询的评论 或者 话题的消息
      sql += " and feedId  = '" + feedId + "'"
    }

    if (updateLevel.nonEmpty) {
      //评论层级，0为消息  1为内容的评论 或者消息的评论   2 以后评论的评论
      sql += " and updateLevel  = '" + updateLevel + "'"
    }

    if (excludeMessageId.nonEmpty) {
      sql += " and messageId not in ('" + excludeMessageId.replaceAll(",", "','") + "')"
    }

    if (messageType.nonEmpty) {
      //取多个消息类型(一般消息列表,取得是转发 或者 原创)
      sql += " and messageType in ('" + messageType.replaceAll(",", "','") + "')"
    }

    if ("2".equals(messageType) && messageId.nonEmpty) {
      //获取消息的转发列表
      sql += " and messageType = 2 and messageId in (select referenceMsgId from msgRelations where referencedMsgId = '" + messageId + "' and opType = 2)"
    }

    if ("1".equals(messageType) && messageId.nonEmpty) {
      //获取消息的评论列表
      sql += " and messageType = 1 and messageId in (select referenceMsgId from msgRelations where referencedMsgId = '" + messageId + "' and opType = 1)"
    }

    if (isRecommend.nonEmpty) {
      //是否推荐数据
      sql += " and isRecommend = '" + isRecommend + "'"
    }

    if (orderInfo.isEmpty) {
      sql += " order by cTime desc "
    } else {
      sql += " order by " + orderInfo
    }
    sql += " limit " + offset + "," + limits

    client.select[MessageInfo](sql){row =>
        val messageId = row("messageId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val messageType = row("messageType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
        val cUserId = row("cUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val collectNum = row("collectNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNum = row("updateNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNumD = row("updateNumD").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNum = row("forwardNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNumD = row("forwardNum").map {
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
        val readNum = row("readNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateType = row("updateType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val threshHold = row("threshHold").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isAnonymous = row("isAnonymous").map {
          case IntValue(str) => str
          case _ => 0
        }.get

         val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val fuzzyImgs = row("fuzzyImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val updateLevel = row("updateLevel").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val isDelete = row("isDelete").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val mImgs = row("mImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get

      val isTop = row("isTop").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isRecommend = row("isRecommend").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val replyMsgId = row("replyMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val replyUserId = row("replyUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val linkTitle = row("linkTitle").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val linkImg = row("linkImg").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val linkUrl = row("linkUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        MessageInfo(UUID.fromString(messageId),feedId,messageType,"", new DateTime(cTime),cUserId,collectNum,updateNum,updateNumD,
        forwardNum,forwardNumD,likeNum,unLikeNum,readNum,updateType,threshHold,isAnonymous,imgUrls,fuzzyImgs,
        updateLevel,isDelete,groupId,mImgs,isTop,isRecommend,replyMsgId,replyUserId,linkTitle,linkImg,linkUrl)
      }.flatMap {
      msgs => Future.collect(msgs.map {
        msg => select.where(_.messageId eqs msg.messageId).get().map {
          case Some(t) => MessageInfo(msg.messageId, msg.feedId, msg.messageType, t.content, msg.cTime, msg.cUserId,
            msg.collectNum, msg.updateNum, msg.updateNumD, msg.forwardNum, msg.forwardNumD, msg.likeNum, msg.unLikeNum,
            msg.readNum, msg.updateType, msg.threshHold, msg.isAnonymous, msg.imgUrls, msg.fuzzyImgs,
            msg.updateLevel, msg.isDelete, msg.groupId, msg.mImgs, msg.isTop, msg.isRecommend, msg.replyMsgId,
            msg.replyUserId, msg.linkTitle, msg.linkImg, msg.linkUrl)
          case None => msg
        }
      })
    }
  }


  //获取关联的主消息信息
  def getReferenceMsg(messageId: UUID): Future[Option[MessageInfo]] = {
    val sql =
      """ select * from messageInfos where  messageId in
      (select referencedMsgId from msgRelations where  referenceMsgId = ?) limit 1 """

    client.prepare(sql).select[MessageInfo](messageId.toString){row =>
        val messageId = row("messageId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val messageType = row("messageType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
        val cUserId = row("cUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val collectNum = row("collectNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNum = row("updateNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNumD = row("updateNumD").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNum = row("forwardNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNumD = row("forwardNum").map {
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
        val readNum = row("readNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateType = row("updateType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val threshHold = row("threshHold").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isAnonymous = row("isAnonymous").map {
          case IntValue(str) => str
          case _ => 0
        }.get

         val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val fuzzyImgs = row("fuzzyImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val updateLevel = row("updateLevel").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val isDelete = row("isDelete").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val mImgs = row("mImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val isTop = row("isTop").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isRecommend = row("isRecommend").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val replyMsgId = row("replyMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val replyUserId = row("replyUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val linkTitle = row("linkTitle").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val linkImg = row("linkImg").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val linkUrl = row("linkUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        MessageInfo(UUID.fromString(messageId),feedId,messageType,"", new DateTime(cTime),cUserId,collectNum,updateNum,updateNumD,
        forwardNum,forwardNumD,likeNum,unLikeNum,readNum,updateType,threshHold,isAnonymous,imgUrls,fuzzyImgs,
        updateLevel,isDelete,groupId,mImgs,isTop,isRecommend,replyMsgId,replyUserId,linkTitle,linkImg,linkUrl)
      }.flatMap {
      msgs => Future.collect(msgs.map {
        msg => select.where(_.messageId eqs msg.messageId).get().map {
          case Some(t) => MessageInfo(msg.messageId, msg.feedId, msg.messageType, t.content, msg.cTime, msg.cUserId,
            msg.collectNum, msg.updateNum, msg.updateNumD, msg.forwardNum, msg.forwardNumD, msg.likeNum, msg.unLikeNum,
            msg.readNum, msg.updateType, msg.threshHold, msg.isAnonymous, msg.imgUrls, msg.fuzzyImgs,
            msg.updateLevel, msg.isDelete, msg.groupId, msg.mImgs, msg.isTop, msg.isRecommend, msg.replyMsgId,
            msg.replyUserId, msg.linkTitle, msg.linkImg, msg.linkUrl)
          case None => msg
        }
      })
    }.map(_.headOption)
  }


  def collMessagesByIdWithUserIds(feedId: String, userIds: String, orderInfo: String, limits: Int, offset: Int): Future[Seq[MessageInfo]] = {
    var query =
      """select * from
                  (
                  select *,CONCAT("Top_",date_format(cTime,'%Y-%m-%d %H:%i:%s')) as sortNo from messageInfos where  isDelete = 0 and  feedId = '""" + feedId +
        """' and updateLevel = 1  AND  isAnonymous = 0 and  cUserId   in ('""" + userIds.replaceAll(",", "','") +
        """')
                  union
                  select *,date_format(cTime,'%Y-%m-%d %H:%i:%s')  as sortNo from  messageInfos where  isDelete = 0 and  feedId = '""" + feedId +
        """' and updateLevel = 1 AND (isAnonymous = 1 or cUserId NOT in ('""" + userIds.replaceAll(",", "','") +
        """'))
                  ) t
        """
    if (orderInfo.isEmpty) {
      query += "order by sortNo desc"
    } else {
      query += "order by " + orderInfo
    }


    query += "   limit " + offset + ", " + limits

    client.select[MessageInfo](query) { row =>
       val messageId = row("messageId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val messageType = row("messageType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
        val cUserId = row("cUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val collectNum = row("collectNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNum = row("updateNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNumD = row("updateNumD").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNum = row("forwardNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNumD = row("forwardNum").map {
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
        val readNum = row("readNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateType = row("updateType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val threshHold = row("threshHold").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isAnonymous = row("isAnonymous").map {
          case IntValue(str) => str
          case _ => 0
        }.get

         val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val fuzzyImgs = row("fuzzyImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val updateLevel = row("updateLevel").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val isDelete = row("isDelete").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val mImgs = row("mImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
      val isTop = row("isTop").map {
          case IntValue(str) => str
          case _ => 0
        }.get

        val isRecommend = row("isRecommend").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val replyMsgId = row("replyMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val replyUserId = row("replyUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val linkTitle = row("linkTitle").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val linkImg = row("linkImg").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val linkUrl = row("linkUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        MessageInfo(UUID.fromString(messageId),feedId,messageType,"", new DateTime(cTime),cUserId,collectNum,updateNum,updateNumD,
        forwardNum,forwardNumD,likeNum,unLikeNum,readNum,updateType,threshHold,isAnonymous,imgUrls,fuzzyImgs,
        updateLevel,isDelete,groupId,mImgs,isTop,isRecommend,replyMsgId,replyUserId,linkTitle,linkImg,linkUrl)
      }.flatMap {
      msgs => Future.collect(msgs.map {
        msg => select.where(_.messageId eqs msg.messageId).get().map {
          case Some(t) => MessageInfo(msg.messageId, msg.feedId, msg.messageType, t.content, msg.cTime, msg.cUserId,
            msg.collectNum, msg.updateNum, msg.updateNumD, msg.forwardNum, msg.forwardNumD, msg.likeNum, msg.unLikeNum,
            msg.readNum, msg.updateType, msg.threshHold, msg.isAnonymous, msg.imgUrls, msg.fuzzyImgs,
            msg.updateLevel, msg.isDelete, msg.groupId, msg.mImgs, msg.isTop, msg.isRecommend, msg.replyMsgId,
            msg.replyUserId, msg.linkTitle, msg.linkImg, msg.linkUrl)
          case None => msg
        }
      })

    }
  }


  def getUserAttentionMsg(userId: String, limits: Int, offset: Int): Future[Seq[MessageInfo]] = {
    val sql =
      """     SELECT
               	*
               FROM
               	messageInfos
               WHERE
               	isDelete = 0
               AND updateLevel = '0'
               AND messageType IN ('0', '2')
               AND EXISTS(
               	SELECT
               		followId
               	FROM
               		userRelations
               	WHERE
               		userId = ?
               	AND rtype = 0
               	AND messageInfos.cUserId = userRelations.followId
               ) order by   cTime desc  limit ?,?"""
    client.prepare(sql).select[MessageInfo](userId.toString,offset,limits){row =>
        val messageId = row("messageId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val messageType = row("messageType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
        val cUserId = row("cUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val collectNum = row("collectNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNum = row("updateNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNumD = row("updateNumD").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNum = row("forwardNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNumD = row("forwardNum").map {
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
        val readNum = row("readNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateType = row("updateType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val threshHold = row("threshHold").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isAnonymous = row("isAnonymous").map {
          case IntValue(str) => str
          case _ => 0
        }.get

         val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val fuzzyImgs = row("fuzzyImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val updateLevel = row("updateLevel").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val isDelete = row("isDelete").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val mImgs = row("mImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val isTop = row("isTop").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val isRecommend = row("isRecommend").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val replyMsgId = row("replyMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val replyUserId = row("replyUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val linkTitle = row("linkTitle").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val linkImg = row("linkImg").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val linkUrl = row("linkUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        MessageInfo(UUID.fromString(messageId),feedId,messageType,"", new DateTime(cTime),cUserId,collectNum,updateNum,updateNumD,
        forwardNum,forwardNumD,likeNum,unLikeNum,readNum,updateType,threshHold,isAnonymous,imgUrls,fuzzyImgs,
        updateLevel,isDelete,groupId,mImgs,isTop,isRecommend,replyMsgId,replyUserId,linkTitle,linkImg,linkUrl)
      }.flatMap {
      msgs => Future.collect(msgs.map {
        msg => select.where(_.messageId eqs msg.messageId).get().map {
          case Some(t) => MessageInfo(msg.messageId, msg.feedId, msg.messageType, t.content, msg.cTime, msg.cUserId,
            msg.collectNum, msg.updateNum, msg.updateNumD, msg.forwardNum, msg.forwardNumD, msg.likeNum, msg.unLikeNum,
            msg.readNum, msg.updateType, msg.threshHold, msg.isAnonymous, msg.imgUrls, msg.fuzzyImgs,
            msg.updateLevel, msg.isDelete, msg.groupId, msg.mImgs, msg.isTop, msg.isRecommend, msg.replyMsgId,
            msg.replyUserId, msg.linkTitle, msg.linkImg, msg.linkUrl)
          case None => msg
        }
      })
    }
  }

  def getHotMsgS(feedId: String, limits: Int): Future[Seq[MessageInfo]] = {
    var sql = " select * from messageInfos where  isDelete = 0  and ( updateLevel = 1 or  updateLevel = 0)  and likeNum > 5"


    if (feedId.nonEmpty) {
      //咨询的评论
      sql += " and feedId  = '" + feedId + "'"
    }

    sql += " order by likeNum desc,cTime desc"
    sql += " limit " + limits

    client.select[MessageInfo](sql){row =>
        val messageId = row("messageId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val feedId = row("feedId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val messageType = row("messageType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
        val cUserId = row("cUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val collectNum = row("collectNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNum = row("updateNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateNumD = row("updateNumD").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNum = row("forwardNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val forwardNumD = row("forwardNum").map {
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
        val readNum = row("readNum").map {
          case LongValue(str) => str
          case _ => 0L
        }.get
         val updateType = row("updateType").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val threshHold = row("threshHold").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isAnonymous = row("isAnonymous").map {
          case IntValue(str) => str
          case _ => 0
        }.get

         val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val fuzzyImgs = row("fuzzyImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val updateLevel = row("updateLevel").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val isDelete = row("isDelete").map {
          case IntValue(str) => str
          case _ => 0
        }.get
          val groupId = row("groupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val mImgs = row("mImgs").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val isTop = row("isTop").map {
          case IntValue(str) => str
          case _ => 0
        }.get
        val isRecommend = row("isRecommend").map {
          case IntValue(str) => str
          case _ => 0
        }.get
         val replyMsgId = row("replyMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val replyUserId = row("replyUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val linkTitle = row("linkTitle").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val linkImg = row("linkImg").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val linkUrl = row("linkUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        MessageInfo(UUID.fromString(messageId),feedId,messageType,"", new DateTime(cTime),cUserId,collectNum,updateNum,updateNumD,
        forwardNum,forwardNumD,likeNum,unLikeNum,readNum,updateType,threshHold,isAnonymous,imgUrls,fuzzyImgs,
        updateLevel,isDelete,groupId,mImgs,isTop,isRecommend,replyMsgId,replyUserId,linkTitle,linkImg,linkUrl)
      }.flatMap {
      msgs => Future.collect(msgs.map {
        msg => select.where(_.messageId eqs msg.messageId).get().map {
          case Some(t) => MessageInfo(msg.messageId, msg.feedId, msg.messageType, t.content, msg.cTime, msg.cUserId,
            msg.collectNum, msg.updateNum, msg.updateNumD, msg.forwardNum, msg.forwardNumD, msg.likeNum, msg.unLikeNum,
            msg.readNum, msg.updateType, msg.threshHold, msg.isAnonymous, msg.imgUrls, msg.fuzzyImgs,
            msg.updateLevel, msg.isDelete, msg.groupId, msg.mImgs, msg.isTop, msg.isRecommend, msg.replyMsgId,
            msg.replyUserId, msg.linkTitle, msg.linkImg, msg.linkUrl)
          case None => msg
        }
      })
    }
  }

  //获取评论消息  opType  1 评论   2 消息点赞   3  评论点赞  4 消息表情  5评论回复
  def getUserNoticeComments(userId: String, time: DateTime): Future[Seq[UserMsgNotice]] = {
    val sql =
      """
          SELECT
        	m3.messageId as msgId,
                	DATE_FORMAT( m3.cTime, '%Y-%m-%d %H:%i:%s') as cTime,
                	m3.cUserId,
               	m3.imgUrls,
               	m3.circleId,
               	m3.opMsgId   ,
                	m3.opUserId,
                	DATE_FORMAT( m3.opTime, '%Y-%m-%d %H:%i:%s')  as opTime ,
               	m4.imgUrls as opImgUrls,
                    '1' as opType,
                    m3.messageId as mainMsgId
        FROM
        	(
        		SELECT
        			m2.messageId,
        			m2.cTime,
        			m2.cUserId,
        			m2.imgUrls,
        			m2.groupId as circleId,
        			mr.referenceMsgId as opMsgId,
        			mr.referenceUserId as opUserId,
        			mr.cTime AS opTime
        		FROM
        			(
        				SELECT
        					m1.messageId,
        					m1.cTime,
        					m1.cUserId,
        					m1.imgUrls,
        					m1.groupId
        				FROM
        					messageInfos m1
        				WHERE
        					m1.isDelete = 0
        				AND m1.cUserId = ?
        				AND m1.messageType = 0
        			)m2
        		LEFT JOIN msgRelations mr ON m2.messageId = mr.referencedMsgId
        		WHERE
        			mr.opType = 1
        		AND mr.cTime > ?
        	)m3
        LEFT JOIN messageInfos m4 ON m3.opMsgId = m4.messageId
        WHERE
        	m4.messageType = 1 and m4.isDelete = 0
         and  m3.opUserId <> ?
      """
    client.prepare(sql).select[UserMsgNotice](userId,time.toDate,userId){ row =>
       val msgId = row("msgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val cTime = row("cTime").map{
         case StringValue(str) => new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse(str)).withZone(DateTimeZone.getDefault)
          case timeValue(str) =>new DateTime(str)
          case TimestampValue(str) =>new DateTime(str)
          case _ =>new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse("2016-08-03 00:00:00")).withZone(DateTimeZone.getDefault)
        }.get
        val cUserId = row("cUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val circleId = row("circleId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val opMsgId = row("opMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get

         val opUserId = row("opUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val opTime =  row("opTime").map{
          case StringValue(str) => new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse(str)).withZone(DateTimeZone.getDefault)
          case timeValue(str) =>new DateTime(str)
          case TimestampValue(str) =>new DateTime(str)
          case _ =>new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse("2016-08-03 00:00:00")).withZone(DateTimeZone.getDefault)
        }.get
         val opImgUrls = row("opImgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val opType = row("opType").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val mainMsgId = row("mainMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        UserMsgNotice(UUID.fromString(msgId),cUserId,cTime,"",imgUrls,
        circleId,opUserId,opTime,opMsgId,"",opImgUrls,opType,UUID.fromString(mainMsgId))
    }.flatMap {
      notices => Future.collect(notices.map {
        notice => val msgF = select.where(_.messageId eqs notice.msgId).get().map {
          case Some(msg) => msg.content
          case None => ""
        }
          val opMsgF = select.where(_.messageId eqs UUID.fromString(notice.opMsgId)).get().map {
            case Some(msg) => msg.content
            case None => ""
          }
          for {
            msg <- msgF
            opMsg <- opMsgF
          } yield (msg, opMsg) match {
            case (msgContent, opMsgContent) => UserMsgNotice(notice.msgId, notice.cUserId, notice.cTime,
              msgContent, notice.imgUrls, notice.circleId, notice.opUserId, notice.opTime, notice.opMsgId,
              opMsgContent, notice.opImgUrls, notice.opType, notice.mainMsgId)
          }
      })
    }
  }

  //获取评论点赞表情消息 opType 1 评论   2 消息点赞   3  评论点赞  4 消息表情  5评论回复
  def getUserNoticeReply(userId: String, time: DateTime): Future[Seq[UserMsgNotice]] = {
    val sql =
      """SELECT
        	m3.messageId AS msgId,
        	DATE_FORMAT( m3.cTime, '%Y-%m-%d %H:%i:%s') as cTime,
        	m3.cUserId,
        	m3.imgUrls,
        	m2.circleId,
        	m2.opMsgId,
        	m2.opUserId,
        	DATE_FORMAT( m2.opTime, '%Y-%m-%d %H:%i:%s') as opTime,
        	m2.opImgUrls,
        	'5' AS opType,
        	(
        		SELECT
        			referencedMsgId
        		FROM
        			msgRelations
        		WHERE
        			referenceMsgId = m3.messageId
        	)AS mainMsgId
        FROM
        	(
        		SELECT
        			m1.messageId AS opMsgId,
        			m1.cTime AS opTime,
        			m1.cUserId AS opUserId,
        			m1.imgUrls AS opImgUrls,
        			m1.groupId AS circleId,
        			m1.replyUserId AS userId,
        			m1.replyMsgId AS msgId
        		FROM
        			messageInfos m1
        		WHERE
        			m1.isDelete = 0
        		AND m1.messageType = 1
        		AND m1.replyUserId = ?
        		AND m1.cTime > ?
        	)m2
        LEFT JOIN messageInfos m3 ON m2.msgId = m3.messageId
        where 	m2.opUserId <> ?
      """
    client.prepare(sql).select[UserMsgNotice](userId,time.toDate,userId){ row =>
       val msgId = row("msgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val cTime = row("cTime").map{
        case StringValue(str) => new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse(str)).withZone(DateTimeZone.getDefault)
          case timeValue(str) =>new DateTime(str)
          case TimestampValue(str) =>new DateTime(str)
          case _ =>new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse("2016-08-03 00:00:00")).withZone(DateTimeZone.getDefault)
        }.get
        val cUserId = row("cUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val circleId = row("circleId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val opUserId = row("opUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val opTime =  row("opTime").map{
         case StringValue(str) => new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse(str)).withZone(DateTimeZone.getDefault)
          case timeValue(str) =>new DateTime(str)
          case TimestampValue(str) =>new DateTime(str)
          case _ =>new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse("2016-08-03 00:00:00")).withZone(DateTimeZone.getDefault)
        }.get
         val opType = row("opType").map {
          case StringValue(str) => str
          case _ => ""
        }.get
          val opMsgId = row("opMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val mainMsgId = row("mainMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        UserMsgNotice(UUID.fromString(msgId),cUserId,cTime,"",imgUrls,circleId,opUserId,opTime,opMsgId
        ,"","",opType,UUID.fromString(mainMsgId))
    }.flatMap {
      notices => Future.collect(notices.map {
        notice =>

          val msgContentF = select.where(_.messageId eqs notice.msgId).get().map {
            case Some(msg) => msg.content
            case None => ""
          }
          val opContentF = select.where(_.messageId eqs UUID.fromString(notice.opMsgId)).get().map {
            case Some(msg) => msg.content
            case None => ""
          }
          for {
            msgContent <- msgContentF
            opContent <- opContentF
          } yield (msgContent, opContent) match {
            case (msgContent, opContent) => UserMsgNotice(notice.msgId, notice.cUserId, notice.cTime,
              msgContent, notice.imgUrls, notice.circleId, notice.opUserId, notice.opTime, notice.opMsgId,
              opContent, notice.opImgUrls, notice.opType, notice.mainMsgId)
          }
      })
    }
  }

  //获取评论点赞表情消息 opType 1 评论   2 消息点赞   3  评论点赞  4 消息表情  5评论回复
  def getUserNoticeEm(userId: String, time: DateTime): Future[Seq[UserMsgNotice]] = {
    val sql =
      """
        SELECT * from (
        SELECT
        	messageInfos.messageId AS msgId,
        	messageInfos.cUserId AS userId,
        	DATE_FORMAT(messageInfos.cTime, '%Y-%m-%d %H:%i:%s') as cTime,
        	messageInfos.imgUrls AS imgUrls,
        	messageInfos.groupId AS circleId,
        	updateLikes.userId AS opUserId,
          DATE_FORMAT(updateLikes.opTime, '%Y-%m-%d %H:%i:%s') AS opTime,
        	'' AS opContent,
        	'2' AS opType,
        	messageInfos.messageId AS mainMsgId
        FROM
        	messageInfos
        LEFT JOIN updateLikes ON messageInfos.messageId = updateLikes.updateId
        WHERE
        	messageInfos.cUserId = ?
        AND messageInfos.isDelete = 0
        AND updateLikes.opTime > ?
        AND messageInfos.messageType = 0
        UNION ALL
        	SELECT
        		messageInfos.messageId AS msgId,
        		messageInfos.cUserId AS userId,
        		DATE_FORMAT(messageInfos.cTime, '%Y-%m-%d %H:%i:%s') as cTime,
        		messageInfos.imgUrls AS imgUrls,
        		messageInfos.groupId AS circleId,
        		updateLikes.userId AS opUserId,
            DATE_FORMAT(updateLikes.opTime, '%Y-%m-%d %H:%i:%s') AS opTime,
        		'' AS opContent,
        		'3' AS opType,
        		(
        			SELECT
        				referencedMsgId
        			FROM
        				msgRelations
        			WHERE
        				referenceMsgId = messageInfos.messageId
        		)AS mainMsgId
        	FROM
        		messageInfos
        	LEFT JOIN updateLikes ON messageInfos.messageId = updateLikes.updateId
        	WHERE
        		messageInfos.cUserId = ?
        	AND messageInfos.isDelete = 0
        	AND updateLikes.opTime > ?
        	AND messageInfos.messageType = 1
        	UNION ALL
        		SELECT
        			messageInfos.messageId AS msgId,
        			messageInfos.cUserId AS userId,
        			DATE_FORMAT(messageInfos.cTime, '%Y-%m-%d %H:%i:%s') AS cTime,
        			messageInfos.imgUrls AS imgUrls,
        			messageInfos.groupId AS circleId,
        			emotionCommunicates.userId AS opUserId,
        			DATE_FORMAT(emotionCommunicates.cTime, '%Y-%m-%d %H:%i:%s') AS opTime,
        			emotionCommunicates.selectId AS opContent,
        			'4' AS opType,
        			messageInfos.messageId AS mainMsgId
        		FROM
        			messageInfos
        		LEFT JOIN emotionCommunicates ON messageInfos.messageId = emotionCommunicates.updateId
        		WHERE
        			messageInfos.cUserId = ?
        		AND messageInfos.isDelete = 0
        		AND emotionCommunicates.cTime > ?
        		AND messageInfos.messageType = 0) t
          where opUserId <> ?
      """
    client.prepare(sql).select[UserMsgNotice](userId,time.toDate,userId,time.toDate,userId,time.toDate,userId){ row =>
       val msgId = row("msgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val cTime = row("cTime").map{
         case StringValue(str) => new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse(str)).withZone(DateTimeZone.getDefault)
          case timeValue(str) =>new DateTime(str)
          case TimestampValue(str) =>new DateTime(str)
          case _ =>new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse("2016-08-03 00:00:00")).withZone(DateTimeZone.getDefault)
        }.get
        val cUserId = row("userId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val imgUrls = row("imgUrls").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val circleId = row("circleId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val opUserId = row("opUserId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val opTime =  row("opTime").map{
          case StringValue(str) => new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse(str))
          case timeValue(str) =>new DateTime(str)
          case TimestampValue(str) =>new DateTime(str)
          case _ =>new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
          parse("2016-08-03 00:00:00"))
        }.get
         val opContent = row("opContent").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val opType = row("opType").map {
          case StringValue(str) => str
          case _ => ""
        }.get
         val mainMsgId = row("mainMsgId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        UserMsgNotice(UUID.fromString(msgId),cUserId,cTime,"",imgUrls,circleId,opUserId,opTime,
        "",opContent,"",opType,UUID.fromString(mainMsgId))
    }.flatMap {
      notices => Future.collect(notices.map {
        notice => select.where(_.messageId eqs notice.msgId).get().map {
          case Some(msg) => msg.content
          case None => ""
        }.map {
          msgContent => UserMsgNotice(notice.msgId, notice.cUserId, notice.cTime,
            msgContent, notice.imgUrls, notice.circleId, notice.opUserId, notice.opTime, notice.opMsgId,
            notice.opContent, notice.opImgUrls, notice.opType, notice.mainMsgId)
        }
      })

    }
  }

}