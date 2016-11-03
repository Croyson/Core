package com.wisedu.next.models

import _root_.java.lang.{Long => JLong}
import java.util.TimeZone

import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.util.Future
import com.wisedu.next.consts.CircleKeys
import com.wisedu.next.services.BaseFunctions
import com.wisedu.next.types.NextTypes.MysqlClient

case class Circle(circleId: String, circleName: String, description: String,
                  circleType: Int, strutsType: Int, iconUrl: String, backImgUrl: String,
                  sortNo: Int, sortStrategy: Int, permissionsLimit: Int,
                  posterGroupId: String, topMessages: String, isAnonymous: Int,
                  isRealNamePost: Int, isRealNameRespond: Int, notice: String, adminUser: String)

abstract class ConcreteCircles(val client: MysqlClient, val redisClient: Client) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getByIdWithCache(circleId: String): Future[Option[Circle]] = {
    val key = CircleKeys.circleId.format(circleId)
    redisClient.get(StringToChannelBuffer(key)).map {
      result => result.map(item => BaseFunctions.channelBuffer2Object[Circle](item))
    }
  }

  def delByIdWithCache(circleId: String): Future[JLong] = {
    val key = CircleKeys.circleId.format(circleId)
    redisClient.del(Seq(StringToChannelBuffer(key)))
  }


  def getRecommendByIdWithCache(collegeId: String): Future[Option[Circle]] = {
    val key = CircleKeys.circleRecommendId.format(collegeId)
    redisClient.get(StringToChannelBuffer(key)).map {
      result => result.map(item => BaseFunctions.channelBuffer2Object[Circle](item))
    }
  }

  def delRecommendByIdWithCache(collegeId: String): Future[JLong] = {
    val key = CircleKeys.circleRecommendId.format(collegeId)
    redisClient.del(Seq(StringToChannelBuffer(key)))
  }
  def setRecommendByIdWithCache(collegeId: String, circle: Circle): Future[Unit] = {
    val key = CircleKeys.circleRecommendId.format(collegeId)
    redisClient.setEx(StringToChannelBuffer(key), 3600, BaseFunctions.object2ChannelBuffer(circle))
  }

  def setByIdWithCache(circleId: String, circle: Circle): Future[Unit] = {
    val key = CircleKeys.circleId.format(circleId)
    redisClient.setEx(StringToChannelBuffer(key), 604800, BaseFunctions.object2ChannelBuffer(circle))
  }

  def getCirclesByKeyWithCache(key: String): Future[Seq[Circle]] = {
    redisClient.get(StringToChannelBuffer(key)).map {
      case Some(result) => BaseFunctions.channelBuffer2Object[Seq[Circle]](result)
      case None => Seq()
    }
  }

  def delCirclesByKeyWithCache(key: String): Future[JLong] = {
    redisClient.del(Seq(StringToChannelBuffer(key)))
  }

  def setCirclesByKeyCache(key: String, circles: Seq[Circle]): Future[Unit] = {
    redisClient.setEx(StringToChannelBuffer(key), 7200, BaseFunctions.object2ChannelBuffer(circles))
  }


  def getById(circleId: String): Future[Option[Circle]] = {
    val query = "SELECT * FROM circles WHERE isDelete = 0 and circleId = '" + circleId + "' limit 1 "
    client.select[Circle](query){ row =>
        val circleId = row("circleId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val circleName = row("circleName").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val description = row("description").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(circleType) = row("circleType").getOrElse(0)
        val IntValue(strutsType) = row("strutsType").getOrElse(0)

        val iconUrl = row("iconUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val backImgUrl = row("backImgUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(sortNo) = row("sortNo").getOrElse(0)
        val IntValue(sortStrategy) = row("sortStrategy").getOrElse(0)
        val IntValue(permissionsLimit) = row("permissionsLimit").getOrElse(0)

        val posterGroupId = row("posterGroupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val topMessages = row("topMessages").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(isAnonymous) = row("isAnonymous").getOrElse(0)
        val IntValue(isRealNamePost) = row("isRealNamePost").getOrElse(0)
        val IntValue(isRealNameRespond) = row("isRealNameRespond").getOrElse(0)
         val notice = row("notice").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val adminUser = row("adminUser").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        Circle(circleId,circleName,description,
          circleType, strutsType,iconUrl, backImgUrl,
          sortNo, sortStrategy,permissionsLimit,
          posterGroupId,topMessages, isAnonymous,
          isRealNamePost,isRealNameRespond,notice,adminUser)
      }.map(_.headOption)
  }

  def delById(circleId: String): Future[Result] = {
    val delSql = "update circles set isDelete = 1 where circleId = '" + circleId + "'"
    client.query(delSql)
  }

  def insCircle(circle: Circle): Future[Result] = {
    val insertSQL =
      """insert into circles(circleId, circleName, description,
                          circleType, strutsType, iconUrl, backImgUrl,
                          sortNo, sortStrategy, permissionsLimit,
                          posterGroupId, topMessages, isAnonymous,
                          isRealNamePost, isRealNameRespond,notice,adminUser)
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    client.prepare(insertSQL)(circle.circleId, circle.circleName, circle.description,
      circle.circleType, circle.strutsType, circle.iconUrl, circle.backImgUrl,
      circle.sortNo, circle.sortStrategy, circle.permissionsLimit,
      circle.posterGroupId, circle.topMessages, circle.isAnonymous,
      circle.isRealNamePost, circle.isRealNameRespond, circle.notice,circle.adminUser)
  }

  def updCircle(circle: Circle): Future[Result] = {
    val sql =
      """update circles  set circleName = ?, description = ?,
                                  circleType = ?, strutsType = ?, iconUrl = ?, backImgUrl = ?,
                                 sortNo = ?, sortStrategy = ?, permissionsLimit = ?,
                                  posterGroupId = ?, topMessages = ?, isAnonymous = ?,
                                 isRealNamePost = ?, isRealNameRespond = ?,notice=?,adminUser=?
                       where circleId = ? """
    client.prepare(sql)(circle.circleName, circle.description,
      circle.circleType, circle.strutsType, circle.iconUrl, circle.backImgUrl,
      circle.sortNo, circle.sortStrategy, circle.permissionsLimit,
      circle.posterGroupId, circle.topMessages, circle.isAnonymous,
      circle.isRealNamePost, circle.isRealNameRespond, circle.notice,circle.adminUser, circle.circleId)
  }

  def collCircles(collegeId: String, circleName: String, circleType: String, strutsType: String, limit: Int, offSet: Int): Future[Seq[Circle]] = {
    var query = "SELECT * FROM circles where isDelete = 0 "
    if (!circleName.isEmpty) {
      query += " and circleName like '%" + circleName + "%'"
    }

    if (!circleType.isEmpty) {
      query += " and circleType in ('" + circleType.replaceAll(",", "','") + "')"
    }

    if (!strutsType.isEmpty) {
      query += " and strutsType in ('" + strutsType.replaceAll(",", "','") + "')"
    }


    if (!collegeId.isEmpty) {
      query += " and (permissionsLimit = 0 or  EXISTS (select circleId from circlePermissions where circlePermissions.circleId = circles.circleId and collegeId = '" + collegeId + "' ))"
    }
    query += " order by sortNo asc limit " + offSet + "," + limit

    client.select[Circle](query) { row =>
      val circleId = row("circleId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val circleName = row("circleName").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val description = row("description").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(circleType) = row("circleType").getOrElse(0)
        val IntValue(strutsType) = row("strutsType").getOrElse(0)

        val iconUrl = row("iconUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val backImgUrl = row("backImgUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(sortNo) = row("sortNo").getOrElse(0)
        val IntValue(sortStrategy) = row("sortStrategy").getOrElse(0)
        val IntValue(permissionsLimit) = row("permissionsLimit").getOrElse(0)

        val posterGroupId = row("posterGroupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val topMessages = row("topMessages").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(isAnonymous) = row("isAnonymous").getOrElse(0)
        val IntValue(isRealNamePost) = row("isRealNamePost").getOrElse(0)
        val IntValue(isRealNameRespond) = row("isRealNameRespond").getOrElse(0)
        val notice = row("notice").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val adminUser = row("adminUser").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        Circle(circleId,circleName,description,
          circleType, strutsType,iconUrl, backImgUrl,
          sortNo, sortStrategy,permissionsLimit,
          posterGroupId,topMessages, isAnonymous,
          isRealNamePost,isRealNameRespond,notice,adminUser)
      }
  }

  def collCircleSize(collegeId: String, circleName: String, circleType: String, strutsType: String): Future[Int] = {
    var query = "SELECT count(1) as value FROM circles where isDelete = 0 "
    if (!circleName.isEmpty) {
      query += " and circleName like '%" + circleName + "%'"
    }

    if (!circleType.isEmpty) {
      query += " and circleType in ('" + circleType.replaceAll(",", "','") + "')"
    }

    if (!strutsType.isEmpty) {
      query += " and strutsType in ('" + strutsType.replaceAll(",", "','") + "')"
    }


    if (!collegeId.isEmpty) {
      query += " and (permissionsLimit = 0 or  EXISTS (select circleId from circlePermissions where circlePermissions.circleId = circles.circleId and collegeId = '" + collegeId + "' ))"
    }
    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }

  def getRecommendCircle(collegeId:String): Future[Option[Circle]] = {
    val query = "select * from circles where circleId in (select  circleId from circlePermissions where collegeId = '"+collegeId+"') and strutsType = 4 and isDelete = 0  limit 1"
    client.select[Circle](query){ row =>
        val circleId = row("circleId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val circleName = row("circleName").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val description = row("description").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(circleType) = row("circleType").getOrElse(0)
        val IntValue(strutsType) = row("strutsType").getOrElse(0)

        val iconUrl = row("iconUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val backImgUrl = row("backImgUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(sortNo) = row("sortNo").getOrElse(0)
        val IntValue(sortStrategy) = row("sortStrategy").getOrElse(0)
        val IntValue(permissionsLimit) = row("permissionsLimit").getOrElse(0)

        val posterGroupId = row("posterGroupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val topMessages = row("topMessages").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(isAnonymous) = row("isAnonymous").getOrElse(0)
        val IntValue(isRealNamePost) = row("isRealNamePost").getOrElse(0)
        val IntValue(isRealNameRespond) = row("isRealNameRespond").getOrElse(0)
        val notice = row("notice").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val adminUser = row("adminUser").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        Circle(circleId,circleName,description,
          circleType, strutsType,iconUrl, backImgUrl,
          sortNo, sortStrategy,permissionsLimit,
          posterGroupId,topMessages, isAnonymous,
          isRealNamePost,isRealNameRespond,notice,adminUser)
      }.map(_.headOption)
  }


  def getByMsgId(msgId: String): Future[Option[Circle]] = {
    val query = "select * from  circles where circleId = (select groupid from messageInfos where messageId = '" + msgId + "') and isDelete = 0  limit 1"
    client.select[Circle](query){ row =>
        val circleId = row("circleId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val circleName = row("circleName").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val description = row("description").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(circleType) = row("circleType").getOrElse(0)
        val IntValue(strutsType) = row("strutsType").getOrElse(0)

        val iconUrl = row("iconUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val backImgUrl = row("backImgUrl").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(sortNo) = row("sortNo").getOrElse(0)
        val IntValue(sortStrategy) = row("sortStrategy").getOrElse(0)
        val IntValue(permissionsLimit) = row("permissionsLimit").getOrElse(0)

        val posterGroupId = row("posterGroupId").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val topMessages = row("topMessages").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        val IntValue(isAnonymous) = row("isAnonymous").getOrElse(0)
        val IntValue(isRealNamePost) = row("isRealNamePost").getOrElse(0)
        val IntValue(isRealNameRespond) = row("isRealNameRespond").getOrElse(0)
        val notice = row("notice").map {
          case StringValue(str) => str
          case _ => ""
        }.get
       val adminUser = row("adminUser").map {
          case StringValue(str) => str
          case _ => ""
        }.get
        Circle(circleId,circleName,description,
          circleType, strutsType,iconUrl, backImgUrl,
          sortNo, sortStrategy,permissionsLimit,
          posterGroupId,topMessages, isAnonymous,
          isRealNamePost,isRealNameRespond,notice,adminUser)
      }.map(_.headOption)
  }

}