package com.wisedu.next.services

import java.util.UUID
import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._
import org.joda.time.DateTime

@Singleton
class MsgBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  //插入消息
  def insMessageInfo(messageInfo: MessageInfo): Future[Result] = {
    appDatabase.messageInfos.insMessageInfo(messageInfo)
  }

  //插入消息的引用关系
  def insMsgRelation(msgRelation: MsgRelation): Future[Result] = {
    appDatabase.messageInfos.insMsgRelation(msgRelation)
  }

  //逻辑删除一个消息
  def delMessageInfo(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.delMessageInfo(messageId)
  }

  //逻辑删除一个消息
  def delMessageInfoByAdmin(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.delMessageInfoByAdmin(messageId)
  }

  //增加阅读数
  def incMsgReadNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.incMsgReadNum(messageId)
  }

  //增加收藏数
  def incMsgCollectNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.incMsgCollectNum(messageId)
  }

  //减少收藏数
  def decMsgCollectNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.decMsgCollectNum(messageId)
  }

  //增加评论数
  def incMsgUpdateNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.incMsgUpdateNum(messageId)
  }

  //减少评论数
  def decMsgUpdateNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.decMsgUpdateNum(messageId)
  }

  //增加转发数
  def incMsgForwardNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.incMsgForwardNum(messageId)
  }

  //减少转发数
  def decMsgForwardNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.decMsgForwardNum(messageId)
  }

  //增加赞数
  def incMsgLikeNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.incMsgLikeNum(messageId)
  }

  //减少赞数
  def decMsgLikeNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.decMsgLikeNum(messageId)
  }

  //增加踩数
  def incMsgUnLikeNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.incMsgUnLikeNum(messageId)
  }

  //减少踩数
  def decMsgUnLikeNum(messageId: UUID): Future[Result] = {
    appDatabase.messageInfos.decMsgUnLikeNum(messageId)
  }

  //获取一个消息
  def getMsgById(messageId: UUID): Future[Option[MessageInfo]] = {
    appDatabase.messageInfos.getMsgById(messageId)
  }

  /*
  * 获取消息(评论)列表
  *
  * @param groupIds  一个或者多个,多个的时候 , 隔开
  * @param feedId   咨询或者话题的编号
  * @param updateLevel 评论等级 0为消息  1为内容的评论 或者消息的评论   2 以后评论的评论
  * @param messageId 消息的编号  获取消息的转发(评论)列表时用到
  * @param messageType 消息类型  0原创 1评论 2转发
  * @param excludeMessageId 需要排除的消息
  * @param isRecommend 是否是推荐
  * @param limits 分页参数
  * @param offset 分页参数
  *
  * */
  def getMsgS(groupIds: String, feedId: String, messageId: String, updateLevel: String,
              messageType: String, excludeMessageId: String, isRecommend: String, orderInfo: String, limits: Int, offset: Int): Future[Seq[MessageInfo]] = {
    appDatabase.messageInfos.getMsgS(groupIds, feedId, messageId, updateLevel,
      messageType, excludeMessageId, isRecommend, orderInfo, limits, offset)
  }

  //获取关联的主消息信息 (通常是转发的时候获取被转发信息)
  def getReferenceMsg(messageId: UUID): Future[Option[MessageInfo]] = {
    appDatabase.messageInfos.getReferenceMsg(messageId)
  }

  def collMessagesByIdWithUserIds(feedId: String, userIds: String, orderInfo: String, limits: Int, offset: Int): Future[Seq[MessageInfo]] = {
    appDatabase.messageInfos.collMessagesByIdWithUserIds(feedId, userIds, orderInfo, limits, offset)
  }

  //老接口换表后转换
  def collUpdatesByIdWithUserIds(feedId: String, userIds: String, orderInfo: String, limits: Int, offset: Int): Future[Seq[FeedUpdate]] = {
    appDatabase.messageInfos.collMessagesByIdWithUserIds(feedId, userIds, orderInfo, limits, offset).flatMap {
      msgs => Future.collect(msgs.map {
        msg =>
          val pmsgIdF = appDatabase.messageInfos.getReferenceMsg(msg.messageId).map {
            case Some(pmsg) => pmsg.messageId
            case None => UUID.randomUUID()
          }
          for {
            pmsgId <- pmsgIdF
          } yield FeedUpdate(msg.messageId, if (msg.feedId.nonEmpty) UUID.fromString(msg.feedId)
          else UUID.randomUUID(), if (msg.cUserId.nonEmpty) UUID.fromString(msg.cUserId) else UUID.randomUUID(), msg.content, msg.cTime, msg.likeNum, msg.unLikeNum, pmsgId, msg.updateLevel,
            msg.imgUrls, msg.updateNum, msg.isAnonymous, msg.updateType, msg.threshHold, msg.fuzzyImgs, msg.isDelete)
      })
    }
  }

  //老接口换表后转换
  def collUpdatesById(id: String, id_type: String, limits: Int, offset: Int): Future[Seq[FeedUpdate]] = {
    val msgsF = if (id_type.equals("feedId")) {
      appDatabase.messageInfos.getMsgS("", id, "", "1",
        "1", "", "", "", limits, offset)
    } else {
      appDatabase.messageInfos.getMsgS("", "", id, "1",
        "1", "", "", "", limits, offset)
    }
    msgsF.flatMap {
      msgs => Future.collect(msgs.map {
        msg =>
          val pmsgIdF = appDatabase.messageInfos.getReferenceMsg(msg.messageId).map {
            case Some(pmsg) => pmsg.messageId
            case None => UUID.randomUUID()
          }
          for {
            pmsgId <- pmsgIdF
          } yield FeedUpdate(msg.messageId, if (msg.feedId.nonEmpty) UUID.fromString(msg.feedId)
          else UUID.randomUUID(), if (msg.cUserId.nonEmpty) UUID.fromString(msg.cUserId) else UUID.randomUUID(), msg.content, msg.cTime, msg.likeNum, msg.unLikeNum, pmsgId, msg.updateLevel,
            msg.imgUrls, msg.updateNum, msg.isAnonymous, msg.updateType, msg.threshHold, msg.fuzzyImgs, msg.isDelete)
      })
    }
  }

  //老接口换表后转换
  def getUpdateById(updateId: UUID): Future[Option[FeedUpdate]] = {
    appDatabase.messageInfos.getMsgById(updateId).flatMap {
      case Some(msg) =>
        val pmsgIdF = appDatabase.messageInfos.getReferenceMsg(msg.messageId).map {
          case Some(pmsg) => pmsg.messageId
          case None => UUID.randomUUID()
        }
        for {
          pmsgId <- pmsgIdF
        } yield Option(FeedUpdate(msg.messageId, if (msg.feedId.nonEmpty) UUID.fromString(msg.feedId)
        else UUID.randomUUID(), if (msg.cUserId.nonEmpty) UUID.fromString(msg.cUserId) else UUID.randomUUID(), msg.content, msg.cTime, msg.likeNum, msg.unLikeNum, pmsgId, msg.updateLevel,
          msg.imgUrls, msg.updateNum, msg.isAnonymous, msg.updateType, msg.threshHold, msg.fuzzyImgs, msg.isDelete))

      case None => Future(None)
    }
  }

  //老接口换表后转换
  def getHotUpdatesByFeedId(feed_id: UUID, limits: Int): Future[Seq[FeedUpdate]] = {
    appDatabase.messageInfos.getHotMsgS(feed_id.toString, limits).flatMap {
      msgs => Future.collect(msgs.map {
        msg =>
          val pmsgIdF = appDatabase.messageInfos.getReferenceMsg(msg.messageId).map {
            case Some(pmsg) => pmsg.messageId
            case None => UUID.randomUUID()
          }
          for {
            pmsgId <- pmsgIdF
          } yield FeedUpdate(msg.messageId, if (msg.feedId.nonEmpty) UUID.fromString(msg.feedId)
          else UUID.randomUUID(), if (msg.cUserId.nonEmpty) UUID.fromString(msg.cUserId) else UUID.randomUUID(), msg.content, msg.cTime, msg.likeNum, msg.unLikeNum, pmsgId, msg.updateLevel,
            msg.imgUrls, msg.updateNum, msg.isAnonymous, msg.updateType, msg.threshHold, msg.fuzzyImgs, msg.isDelete)
      })
    }
  }

  //获取用户发的新鲜事
  def getUserMsgs(userId: String, isDelete: String, isAnonymous: String, limits: Int, offset: Int): Future[Seq[MessageInfo]] = {
    appDatabase.messageInfos.getUserMsgs(userId, isDelete, isAnonymous, limits, offset)
  }

  //改变圈子的置顶状态
  def changeMsgTop(messageId: UUID, topStatus: Int): Future[Result] = {
    appDatabase.messageInfos.changeMsgTop(messageId, topStatus)
  }


  /*
  * 根据消息编码获取消息信息
  *
  * @param messageIds  一个或者多个,多个的时候 , 隔开
  * @param orderInfo   排序信息
  *
  * */
  def getMsgByIds(messageIds: String, orderInfo: String): Future[Seq[MessageInfo]] = {
    appDatabase.messageInfos.getMsgByIds(messageIds, orderInfo)
  }

  //获取用户关注咨询
  def getUserAttentionMsg(userId: String, limits: Int, offset: Int): Future[Seq[MessageInfo]] = {
    appDatabase.messageInfos.getUserAttentionMsg(userId, limits, offset)
  }

  //更新消息的状态
  def updateMsgRecommend(msgId: String, status: Int): Future[Result] = {
    appDatabase.messageInfos.updateMsgRecommend(msgId, status)
  }

  //获取评论点赞表情消息 opType 1 评论   2 消息点赞   3  消息表情  4 评论点赞 5消息回復
  def getUserNoticeEm(userId: String, time: DateTime): Future[Seq[UserMsgNotice]] = {
    appDatabase.messageInfos.getUserNoticeEm(userId, time)
  }

  //获取评论点赞表情消息 opType 1 评论   2 消息点赞   3  消息表情  4 评论点赞 5消息回復
  def getUserNoticeComments(userId: String, time: DateTime): Future[Seq[UserMsgNotice]] = {
    appDatabase.messageInfos.getUserNoticeComments(userId, time)
  }

  //获取评论点赞表情回复消息 opType 1 评论   2 消息点赞   3  消息表情  4 评论点赞 5消息回復
  def getUserNoticeReply(userId: String, time: DateTime): Future[Seq[UserMsgNotice]] = {
    appDatabase.messageInfos.getUserNoticeReply(userId, time)
  }

  //获取用户消息 opType 1 评论   2 消息点赞   3  消息表情  4 评论点赞 5消息回復
  def getUserNotice(userId: String, time: DateTime): Future[Seq[UserMsgNotice]] = {
    val emF = appDatabase.messageInfos.getUserNoticeEm(userId, time)
    val commentsF = appDatabase.messageInfos.getUserNoticeComments(userId, time)
    val replyF = appDatabase.messageInfos.getUserNoticeReply(userId, time)
    for {
      em <- emF
      comments <- commentsF
      reply <- replyF
    } yield (em, comments, reply) match {
      case (em, comments, reply) => em ++ comments ++ reply
    }
  }

}