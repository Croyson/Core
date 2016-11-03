package com.wisedu.next.services

import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._

@Singleton
class GroupFeedsBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  def getGroupFeedById(groupId: String, feedId: String): Future[Option[GroupFeed]] = {
    appDatabase.groupFeeds.getById(groupId,feedId)
  }

  def delGroupFeedById(groupId: String, feedId: String): Future[Result] = {
    appDatabase.groupFeeds.delById(groupId,feedId)
  }

  def delGroupFeedByFeedId(feedId: String): Future[Result] = {
    appDatabase.groupFeeds.delByFeedId(feedId)
  }

  def delGroupFeedByGroupId(groupId: String): Future[Result] = {
    appDatabase.groupFeeds.delByGroupId(groupId)
  }

  def updGroupFeed(groupId: String, feedId: String, status: Int): Future[Result] = {
    appDatabase.groupFeeds.updGroupFeed(groupId,feedId,status)
  }

  def collGroupFeeds(groupId: String, limits: Int, offset: Int): Future[Seq[GroupFeed]] = {
    appDatabase.groupFeeds.collGroupFeeds(groupId,limits,offset)
  }

  def collGroupFeedSize(groupId: String): Future[Int] = {
    appDatabase.groupFeeds.collGroupFeedSize(groupId)
  }

  def collGroupFeedPageList(groupId: String, limits: Int, offset: Int): Future[(Seq[GroupFeed],Int)] = {
    val groupFeedsF =  appDatabase.groupFeeds.collGroupFeeds(groupId,limits,offset)
    val groupFeedSizeF = appDatabase.groupFeeds.collGroupFeedSize(groupId)
    for {
      groupFeeds <- groupFeedsF
      groupFeedSize <- groupFeedSizeF
    } yield (groupFeeds, groupFeedSize)
  }

  def getGroupFeedsByFeedId(feedId: String): Future[Seq[GroupFeed]] = {
   appDatabase.groupFeeds.getGroupFeedsByFeedId(feedId)
  }
  def insGroupFeed(groupFeed: GroupFeed): Future[Result] = {
    appDatabase.groupFeeds.insGroupFeed(groupFeed)
  }

}