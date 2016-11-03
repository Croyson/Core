package com.wisedu.next.services

import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models.{AppDatabase, Push}

/**
 * Version: 1.1
 * Author: croyson
 * Time: 16/5/20 下午2:25
 * Desc:
 */

@Singleton
class PushBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  def insPush(push: Push): Future[Result] = {
    appDatabase.pushs.insPush(push)
  }

  def updPush(push: Push): Future[Result] = {
    appDatabase.pushs.updPush(push)
  }

  def delPush(pushId: String): Future[Result] = {
    appDatabase.pushs.delPush(pushId)
  }

  def getPushById(pushId: String): Future[Option[Push]] = {
    appDatabase.pushs.getById(pushId)
  }

  def upStatus(pushId: String, status: Int): Future[Result] = {
    appDatabase.pushs.upStatus(pushId, status)
  }



  def collPushs(status: String, alterContent: String, limits: Int, offset: Int): Future[Seq[Push]] = {
    appDatabase.pushs.collTags(status, alterContent, limits, offset)
  }

  def collPushsSize(status: String, alterContent: String): Future[Int] = {
    appDatabase.pushs.collTagsSize(status, alterContent)
  }

  def collPushsPageList(status: String, alterContent: String, limits: Int, offset: Int): Future[(Seq[Push], Int)] = {
    val pushsF = appDatabase.pushs.collTags(status, alterContent, limits, offset)
    val pushsSizeF = appDatabase.pushs.collTagsSize(status, alterContent)
    for {
      pushs <- pushsF
      pushsSize <- pushsSizeF
    } yield (pushs, pushsSize)
  }

}
