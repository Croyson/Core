package com.wisedu.next.services

import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._

@Singleton
class EmotionService {

  @Inject @NewSqlDatabase var appDatabase: AppDatabase = _

  def getEmotionByModeId(modeId: String): Future[Seq[Emotion]] = {
    appDatabase.emotions.getByModeId(modeId)
  }

  def getEmotionCommunicatesByFeedId(feedId: String): Future[Seq[EmotionCommunicate]] = {
    appDatabase.emotionCommunicate.getByFeedId(feedId)
  }
  def getEmotionCommunicatesByUpdateId(updateId: String): Future[Seq[EmotionCommunicate]] = {
    appDatabase.emotionCommunicate.getByUpdateId(updateId)
  }
  def insEmotionCommunicate(emotionCommunicate: EmotionCommunicate): Future[Result] = {
    appDatabase.emotionCommunicate.insEmotionCommunicate(emotionCommunicate)
  }
  def collEmotionSize(id: String, method: String, model_id: String): Future[Seq[EmotionTypeStats]] = {
    appDatabase.emotionCommunicate.collEmotionSize(id, method, model_id)
  }

  def getByModeIdAndSelectId(modelId: String, selectId: String): Future[Option[Emotion]] ={
    appDatabase.emotions.getByModeIdAndSelectId(modelId, selectId)
  }

  def collectUserById(modelId: String, selectId: String, method: String, id: String,limits: Int, offset: Int): Future[Seq[User]] = {
    appDatabase.emotionCommunicate.collectUserById(modelId, selectId, method, id,limits,offset)
  }

  def getByUpdateIdFeedIdUserId(updateId: String, feedId: String, userId: String): Future[Option[EmotionCommunicate]] = {
    appDatabase.emotionCommunicate.getByUpdateIdFeedIdUserId(updateId, feedId, userId)
  }

  def getByContentUserId(id: String,userId:String,opType:Int): Future[Option[EmotionCommunicate]] = {
    appDatabase.emotionCommunicate.getByContentUserId(id,userId,opType)
  }
}