package com.wisedu.next.services

import java.util.UUID
import javax.inject.{Inject, Singleton}

import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._
import com.twitter.finagle.exp.mysql._

@Singleton
class UpdateBaseService {

  @Inject @NewSqlDatabase var appDatabase: AppDatabase = _

  def insUpdateLike(updateLike: UpdateLike): Future[Result] = {
    appDatabase.updateLikes.insUpdateLike(updateLike)
  }

  def getById(updateId: UUID): Future[Option[FeedUpdate]] = {
    appDatabase.feedUpdates.getById(updateId)
  }

  def insUpdate(update: FeedUpdate): Future[Result] = {
    appDatabase.feedUpdates.insFeedUpdate(update)
  }

  def getHotUpdatesByFeedId(feed_id: UUID, limits: Int): Future[Seq[FeedUpdate]] = {
    appDatabase.feedUpdates.getHotUpdatesByFeedId(feed_id, limits)
  }

  def collUpdatesById(id: String, id_type: String, limits: Int, offset: Int): Future[Seq[FeedUpdate]] = {
    appDatabase.feedUpdates.collUpdatesById(id, id_type, limits, offset)
  }

  def getSubUpdateNumById(id: UUID): Future[Long] = {
    appDatabase.feedUpdates.getSubUpdateNumById(id)
  }

  def incSubUpdateNum(updateId: UUID):Future[Result] = {
    appDatabase.feedUpdates.incSubUpdateNum(updateId)
  }

  def decSubUpdateNum(updateId: UUID):Future[Result] = {
    appDatabase.feedUpdates.decSubUpdateNum(updateId)
  }

  def incFeedUpdateLikeNum(updateId: UUID): Future[Result] = {
    appDatabase.feedUpdates.incFeedUpdateLikeNum(updateId)
  }

  def decFeedUpdateLikeNum(updateId: UUID): Future[Result] = {
    appDatabase.feedUpdates.decFeedUpdateLikeNum(updateId)
  }

  def incFeedUpdateUnLikeNum(updateId: UUID): Future[Result] = {
    appDatabase.feedUpdates.incFeedUpdateUnLikeNum(updateId)
  }

  def decFeedUpdateUnLikeNum(updateId: UUID): Future[Result] = {
    appDatabase.feedUpdates.decFeedUpdateUnLikeNum(updateId)
  }

  def updUpdateLike(update: UpdateLike): Future[Result] = {
    appDatabase.updateLikes.updUpdateLike(update)
  }

  def getUpdateLikeById(userId: UUID, updateId: UUID): Future[Option[UpdateLike]] = {
    appDatabase.updateLikes.getById(userId, updateId)
  }

  def getLikeById(userId: UUID, updateId: UUID, opType: Int): Future[Option[UpdateLike]] = {
    appDatabase.updateLikes.getLikeById(userId, updateId, opType)
  }

  def collUpdatesByIdWithUserIds(feedId: String, userIds: String, limits: Int, offset: Int): Future[Seq[FeedUpdate]] ={
    appDatabase.feedUpdates.collUpdatesByIdWithUserIds(feedId,userIds,limits,offset)
  }

  def delUpdatesLogicById(updateId: UUID,userId:String): Future[Result] = {
    appDatabase.feedUpdates.delLogicById(updateId,userId)
  }
}