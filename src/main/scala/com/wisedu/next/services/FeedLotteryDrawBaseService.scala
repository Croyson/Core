package com.wisedu.next.services

import java.util.UUID
import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models.{AppDatabase, FeedLotteryDraw}

/**
 * Version: 1.1
 * Author: pattywgm
 * Time: 16/5/20 下午2:25
 * Desc:
 */

@Singleton
class FeedLotteryDrawBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  def insFeedLotteryDraw(feedLotteryDraw: FeedLotteryDraw): Future[Result] = {
    appDatabase.feedLotteryDraw.insFeedLotteryDraw(feedLotteryDraw)
  }

  def updFeedLotteryDraw(feedLotteryDraw: FeedLotteryDraw): Future[Result] = {
    appDatabase.feedLotteryDraw.updFeedLotteryDraw(feedLotteryDraw)
  }

  def updFeedId(lotteryDrawId: String, feedId: String): Future[Result] = {
    appDatabase.feedLotteryDraw.updFeedId(lotteryDrawId, feedId)
  }


  def getByFeedId(feedId: String): Future[Option[FeedLotteryDraw]] = {
    appDatabase.feedLotteryDraw.getByFeedId(feedId)
  }

  def getById(lotteryDrawId: String): Future[Option[FeedLotteryDraw]] = {
    appDatabase.feedLotteryDraw.getById(lotteryDrawId)
  }

  def delById(lotteryDrawId: String): Future[Result] = {
    appDatabase.feedLotteryDraw.getById(lotteryDrawId).map {
      case Some(draw) => {
        appDatabase.feeds.updLotteryDrawId(UUID.fromString(draw.feedId), "")
      }
      case None =>
    }
    appDatabase.feedLotteryDraw.delById(lotteryDrawId)
  }

  def delByFeedId(feedId: UUID): Future[Result] = {
    appDatabase.feedLotteryDraw.delByFeedId(feedId)
    appDatabase.feeds.updLotteryDrawId(feedId, "")
  }

  def collFeedLotteryDraws(limit: Int, offset: Int): Future[Seq[FeedLotteryDraw]] = {
    appDatabase.feedLotteryDraw.collFeedLotteryDraws(limit, offset)
  }

  def collFeedLotteryDrawSize(): Future[Int] = {
    appDatabase.feedLotteryDraw.collFeedLotteryDrawSize()
  }

  def collFeedLotteryDrawPageList(limit: Int, offset: Int): Future[(Seq[FeedLotteryDraw], Int)] = {
    val listF = appDatabase.feedLotteryDraw.collFeedLotteryDraws(limit, offset)
    val sizeF = appDatabase.feedLotteryDraw.collFeedLotteryDrawSize()

    for {
      list <- listF
      size <- sizeF
    } yield (list, size)
  }

}
