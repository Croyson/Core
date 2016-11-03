package com.wisedu.next.services

import javax.inject.{Inject, Singleton}

import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models.{AppDatabase, FeedAndStats}

/**
  * Version: 1.1
  * Author: pattywgm 
  * Time: 16/6/2 上午10:03
  * Desc:
  */

@Singleton
class FeedAndStatsBaseService {
  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  def collFeeds(groupId: String, tag: String, state: String,
                orderInfo: String, offSet: Int, limits: Int,userId:String): Future[Seq[FeedAndStats]] = {
    if (!groupId.isEmpty) {
      appDatabase.groups.getById(groupId).map {
        case Some(group) =>(group.feedTypeCL, group.tagCL,group.strutsType.toString,userId)
        case None => ("", "","","")
      }.flatMap {
        case (feedTypeCL,tagCL,strutsType,userId) => appDatabase.feedAndStats.collFeeds(groupId,strutsType, tag, feedTypeCL, tagCL, state, orderInfo,userId, limits, offSet)
      }
    } else {
      appDatabase.feedAndStats.collFeeds(groupId,"", tag, "", "", state, orderInfo,"", limits, offSet)
    }
  }
}
