package com.wisedu.next.services

import java.util.UUID
import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.websudos.phantom.dsl._
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._

@Singleton
class FeedBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  // 获取媒体号下所有资讯
  def getFeedsBySerId(serId: String, status: String, limit: Int, offset: Int): Future[Seq[SqlFeed]] = {
    appDatabase.feeds.getFeedsBySerId(serId, status, limit, offset)
  }

  // 根据feedId获取资讯统计数据
  def getFeedStatsById(feedId: UUID): Future[Option[FeedStat]] = {
    appDatabase.feedStats.getById(feedId)
  }

  // 内容喜欢
  def insFeedLike(feedLike: FeedLike): Future[Result] = {
    appDatabase.feedLikes.insFeedLike(feedLike)
  }

  def updFeedLike(feedLike: FeedLike): Future[Result] = {
    appDatabase.feedLikes.updFeedLike(feedLike)
  }

  def getFeedLikeById(feedId: UUID, userId: UUID): Future[Option[FeedLike]] = {
    appDatabase.feedLikes.getById(userId, feedId)
  }

  def incFeedLikeNum(feedId: UUID): Future[Result] = {
    appDatabase.feedStats.incFeedLikeNum(feedId)
  }

  def decFeedLikeNum(feedId: UUID): Future[Result] = {
    appDatabase.feedStats.decFeedLikeNum(feedId)
  }

  def incFeedUnlikeNum(feedId: UUID): Future[Result] = {
    appDatabase.feedStats.incFeedUnLikeNum(feedId)
  }

  def decFeedUnlikeNum(feedId: UUID): Future[Result] = {
    appDatabase.feedStats.decFeedUnLikeNum(feedId)
  }

  // 内容收藏
  def insFeedCollect(feedCollect: FeedCollect): Future[Result] = {
    appDatabase.feedCollects.insFeedCollect(feedCollect)
  }

  def updFeedCollect(feedCollect: FeedCollect): Future[Result] = {
    appDatabase.feedCollects.updFeedCollect(feedCollect)
  }

  def incFeedCollectNum(feedId: UUID): Future[Result] = {
    appDatabase.feedStats.incFeedCollectNum(feedId)
  }

  // 内容阅读
  def incFeedReadNum(feedId: UUID): Future[Result] = {
    appDatabase.feedStats.incFeedReadNum(feedId)
  }

  def getFeedReadStatsById(userId: String, feedId: UUID) = {
    appDatabase.feedReadStats.getById(userId, feedId)
  }

  def insFeedReadStat(feedReadStat: FeedReadStat): Future[Result] = {
    appDatabase.feedReadStats.insFeedReadStat(feedReadStat)
  }

  def updFeedReadStat(feedReadStat: FeedReadStat): Future[Result] = {
    appDatabase.feedReadStats.updFeedReadStat(feedReadStat)
  }

  def insFeedReadDetails(feedReadDetail: FeedReadDetail): Future[ResultSet] = {
    appDatabase.feedReadDetails.insFeedReadDetails(feedReadDetail)
  }

  // 内容分享
  def insFeedShare(feedShare: FeedShare): Future[ResultSet] = {
    appDatabase.feedShares.insFeedShare(feedShare)
  }

  def incFeedShareNum(feedId: UUID): Future[Result] = {
    appDatabase.feedStats.incFeedShareNum(feedId)
  }

  //内容添加
  def insFeed(feed: Feed): Future[Result] = {
    appDatabase.feeds.insFeed(feed).flatMap {
      rst => appDatabase.feedStats.insFeedStat(FeedStat(feed.feedId, 0, 0, 0, 0, 0, 0, 0, 0, 0)) //增加统计信息默认值
    }
  }

  //内容修改
  def updFeed(feed: Feed): Future[Result] = {
    appDatabase.feeds.updFeed(feed)
  }

  //获取内容
  def getFeedById(feedId: UUID): Future[Option[Feed]] = {
    appDatabase.feeds.getById(feedId)
  }

  //更新状态
  def updStatus(feedId: UUID, status: Int): Future[Result] = {
    appDatabase.feeds.updStatus(feedId, status)
  }

  //删除内容
  def delFeed(feedId: UUID): Future[Result] = {
    appDatabase.feeds.delFeed(feedId)
    appDatabase.feedStats.delById(feedId)
    appDatabase.feedPermissions.delByFeedId(feedId)
    appDatabase.feedTags.delFeedTagsByFeedId(feedId)
    appDatabase.groupFeeds.delByFeedId(feedId.toString)
  }

  //查询内容
  def collFeeds(viewStyle:String,title: String, source: String, groupId: String, tag: String, state: String,
                orderInfo: String, offSet: Int, limits: Int): Future[Seq[SqlFeed]] = {
    if (!groupId.isEmpty) {
      appDatabase.groups.getById(groupId).map {
        case Some(group) => (group.feedTypeCL, group.tagCL, group.strutsType.toString)
        case None => ("", "", "")
      }.flatMap {
        case (feedTypeCL, tagCL, strutsType) => appDatabase.feeds.collFeeds(viewStyle,title, source, groupId, strutsType, tag, feedTypeCL, tagCL, state, orderInfo, offSet, limits)
      }
    } else {
      appDatabase.feeds.collFeeds(viewStyle,title, source, groupId, "", tag, "", "", state, orderInfo, offSet, limits)
    }
  }

  //查询内容数量
  def collFeedSize(viewStyle:String,title: String, source: String, groupId: String, tag: String, state: String,
                   orderInfo: String): Future[Int] = {
    if (!groupId.isEmpty) {
      appDatabase.groups.getById(groupId).map {
        case Some(group) => (group.feedTypeCL, group.tagCL, group.strutsType.toString)
        case None => ("", "", "")
      }.flatMap {
        case (feedTypeCL, tagCL, strutsType) => appDatabase.feeds.collFeedSize(viewStyle,title, source, groupId, strutsType, tag, feedTypeCL, tagCL, state, orderInfo)
      }
    } else {
      appDatabase.feeds.collFeedSize(viewStyle,title, source, groupId, "", tag, "", "", state, orderInfo)
    }
  }

  //查询内容分页信息
  def collFeedPageList(viewStyle:String,title: String, source: String, groupId: String, tag: String, state: String,
                       orderInfo: String, offSet: Int, limits: Int): Future[(Seq[SqlFeed], Int)] = {
    if (groupId.isEmpty) {
      val feedsF = appDatabase.feeds.collFeeds(viewStyle,title, source, groupId, "", tag, "", "", state, orderInfo, offSet, limits)
      val feedSizeF = appDatabase.feeds.collFeedSize(viewStyle,title, source, "", groupId, tag, "", "", state, orderInfo)
      for {
        feeds <- feedsF
        feedSize <- feedSizeF
      } yield (feeds, feedSize)
    } else {
      appDatabase.groups.getById(groupId).map {
        case Some(group) => (group.feedTypeCL, group.tagCL, group.strutsType.toString)
        case None => ("", "", "")
      }.flatMap {
        case (feedTypeCL, tagCL, strutsType) =>
          val feedsF = appDatabase.feeds.collFeeds(viewStyle,title, source, groupId, strutsType, tag, feedTypeCL, tagCL, state, orderInfo, offSet, limits)
          val feedSizeF = appDatabase.feeds.collFeedSize(viewStyle,title, source, groupId, strutsType, tag, feedTypeCL, tagCL, state, orderInfo)
          for {
            feeds <- feedsF
            feedSize <- feedSizeF
          } yield (feeds, feedSize)

      }
    }
  }

  //插入内容标签
  def addFeedTag(feedTag: FeedTag): Future[Result] = {
    appDatabase.feedTags.insFeedTag(feedTag)
  }

  def getFeedTagsByFeedId(feedId: UUID): Future[Seq[FeedTag]] = {
    appDatabase.feedTags.getFeedTagsByFeedId(feedId)
  }

  def delFeedTagsByFeedId(feedId: UUID): Future[Result] = {
    appDatabase.feedTags.delFeedTagsByFeedId(feedId)
  }

  //插入内容学校限制
  def insFeedPermission(feedPermission: FeedPermission): Future[Result] = {
    appDatabase.feedPermissions.insFeedPermission(feedPermission)
  }

  //查询内容学校限制
  def getFeedPermissionsByFeedId(feedId: UUID): Future[Seq[FeedPermission]] = {
    appDatabase.feedPermissions.getByFeedId(feedId)
  }

  //删除内容学校限制
  def delFeedPermissionsByFeedId(feedId: UUID): Future[Result] = {
    appDatabase.feedPermissions.delByFeedId(feedId)
  }

  def insFeedStat(feedStat: FeedStat): Future[Result] = {
    appDatabase.feedStats.insFeedStat(feedStat)
  }

  // 内容评论
  def incFeedUpdateNum(feedId: UUID): Future[Result] = {
    appDatabase.feedStats.incFeedUpdateNum(feedId)
  }
  def decFeedUpdateNum(feedId: UUID): Future[Result] = {
    appDatabase.feedStats.decFeedUpdateNum(feedId)
  }

  def getUpdateNumById(feedId: UUID): Future[Option[Long]] = {
    appDatabase.feedStats.getUpdateNumById(feedId)
  }

  def getFeedIdByTagIds(tagIds: String, limits: Int, offset: Int): Future[Seq[String]] = {
    appDatabase.feedTags.getFeedIdByTagIds(tagIds, limits, offset)
  }

  def getFeedsByViewStyle(feedIds: String, viewStyle: String): Future[Seq[SqlFeed]] = {
    appDatabase.feeds.getFeedsByViewStyle(feedIds, viewStyle)
  }

  def sortFeedsByMTime(feedIds: Seq[String]): Future[Seq[String]] = {
    appDatabase.feeds.sortFeedsByMTime(feedIds)
  }

  def getFeedLotteryByFeedId(feedId: String): Future[Option[FeedLotteryDraw]] = {
    appDatabase.feedLotteryDraw.getByFeedId(feedId)
  }

  def updGroups(feedId: UUID, group1: String, group2: String, group3: String, group4: String, group5: String, group6: String): Future[Result] = {
    appDatabase.feeds.updGroups(feedId, group1, group2, group3, group4, group5, group6)
  }

  def toSqlFeed(feed: Feed): SqlFeed = {
    appDatabase.feeds.toSqlFeed(feed)
  }

  def insFeedback(feedback: Feedback): Future[ResultSet] = {
    appDatabase.feedbacks.insFeedback(feedback)
  }

  def getUpdateLikeNumById(id: String): Future[Long] = {
    appDatabase.feedUpdates.getUpdateLikeNumById(id)
  }

  def getLikeNumById(feed_id: UUID): Future[Long] = {
    appDatabase.feedStats.getLikeNumById(feed_id)
  }

  def decFeedShareNum(feedId: UUID): Future[Result] = {
    appDatabase.feedStats.decFeedShareNum(feedId)
  }

  def delFeedShare(shareId: UUID): Future[ResultSet] = {
    appDatabase.feedShares.delFeedShare(shareId)
  }

  def getByIpaddr(ipAddr: String, feedId: UUID): Future[Option[FeedReadStat]] = {
    appDatabase.feedReadStats.getByIpaddr(ipAddr, feedId)
  }

  def updFeedReadStatByIpAddr(feedReadStat: FeedReadStat): Future[Result] = {
    appDatabase.feedReadStats.updFeedReadStatByIpAddr(feedReadStat)
  }

  def collFeedsByUserId(userId: UUID, status: Int, limits: Int, offset: Int): Future[Seq[SqlFeed]] = {
    appDatabase.feeds.collFeedsByUserId(userId, status, limits, offset)
  }
}