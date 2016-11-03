package com.wisedu.next.models

import _root_.java.lang.{Long => JLong}

import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.util.Future
import com.wisedu.next.consts.CollegeKeys
import com.wisedu.next.services.BaseFunctions
import com.wisedu.next.types.NextTypes.MysqlClient

case class College(collegeId: String, name: String, eName: String, shortName: String, imgUrl: String,
                   lat: Float, lng: Float, addr: String, userNum: Long, region: String, sortNo: Int)

abstract class ConcreteColleges(val client: MysqlClient, val redisClient: Client) {

  def getByIdWithCache(collegeId: String): Future[Option[College]] = {
    val key = CollegeKeys.CollegeId.format(collegeId)
    redisClient.get(StringToChannelBuffer(key)).map {
      result => result.map(item => BaseFunctions.channelBuffer2Object[College](item))
    }
  }

  def setByIdWithCache(college: College): Future[Unit] = {
    val key = CollegeKeys.CollegeId.format(college.collegeId)
    redisClient.setEx(StringToChannelBuffer(key), 60 * 24, BaseFunctions.object2ChannelBuffer(college))
  }

  def delByIdWithCache(collegeId: String): Future[JLong] = {
    val key = CollegeKeys.CollegeId.format(collegeId)
    redisClient.del(Seq(StringToChannelBuffer(key)))
  }

  def getAllCollegesWithCache: Future[Seq[College]] = {
    val key = CollegeKeys.Colleges
    redisClient.get(StringToChannelBuffer(key)).map {
      case Some(result) => BaseFunctions.channelBuffer2Object[Seq[College]](result)
      case None => Seq()
    }
  }

  def setAllCollegesWithCache(colleges: Seq[College]): Future[Unit] = {
    val key = CollegeKeys.Colleges
    redisClient.setEx(StringToChannelBuffer(key), 60 * 24 * 7, BaseFunctions.object2ChannelBuffer(colleges))
  }

  def delAllCollegesWithCache: Future[JLong] = {
    val key = CollegeKeys.Colleges
    redisClient.del(Seq(StringToChannelBuffer(key)))
  }


  def getById(collegeId: String): Future[Option[College]] = {
    val query = "SELECT * FROM colleges WHERE collegeId = '" + collegeId.toString + "' order by sortNo desc limit 1 "
    client.select[College](query) { row =>
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val eName = row("eName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val shortName = row("shortName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val FloatValue(lat) = row("lat").getOrElse("")
      val FloatValue(lng) = row("lng").getOrElse("")
      val addr = row("addr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val LongValue(userNum) = row("userNum").getOrElse("")
      val region = row("region").map {
        case StringValue(str) => str
        case _ => "南京"
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse("")
      College(collegeId, name, eName, shortName, imgUrl, lat, lng, addr, userNum, region,sortNo)
    }.map(_.headOption)
  }

  def delById(collegeId: String): Future[Result] = {
    val delSql = "delete from colleges where collegeId = '" + collegeId.toString + "'"
    client.query(delSql)
  }

  def insCollege(college: College): Future[Result] = {
    val insertSql =
      """INSERT INTO colleges (collegeId, name, eName, shortName, imgUrl, lat, lng,
        addr, userNum, region,sortNo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)"""
    val ps = client.prepare(insertSql)
    ps(college.collegeId.toString, college.name, college.eName, college.shortName, college.imgUrl, college.lat,
      college.lng, college.addr, college.userNum, college.region, college.sortNo)
  }

  def updCollege(college: College): Future[Result] = {
    val updSql =
      """update colleges set name = ?, eName = ?, shortName = ?, imgUrl = ?, lat = ?,
                lng = ?, addr = ?, userNum = ?, region = ?,sortNo = ?  where collegeId = ?"""
    val ps = client.prepare(updSql)
    ps(college.name, college.eName, college.shortName, college.imgUrl, college.lat, college.lng, college.addr, college.userNum,
      college.region, college.sortNo, college.collegeId.toString)
  }

  def collCollegeSize(region: String): Future[Int] = {
    val query = "SELECT count(1) as value FROM colleges WHERE region like '%" + region + "%'"

    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }


  def collColleges(limits: Int, offset: Int, region: String): Future[Seq[College]] = {
    val query = "SELECT * FROM colleges WHERE region like '%" + region + "%'order by sortNo desc limit " + offset + ", " + limits

    client.select[College](query) { row =>
     val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val eName = row("eName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val shortName = row("shortName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val FloatValue(lat) = row("lat").getOrElse("")
      val FloatValue(lng) = row("lng").getOrElse("")
      val addr = row("addr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val LongValue(userNum) = row("userNum").getOrElse("")
      val region = row("region").map {
        case StringValue(str) => str
        case _ => "南京"
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse("")
      College(collegeId, name, eName, shortName, imgUrl, lat, lng, addr, userNum, region,sortNo)
    }
  }


  def collAllColleges: Future[Seq[College]] = {
    val query = "SELECT * FROM colleges "

    client.select[College](query) { row =>
     val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val eName = row("eName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val shortName = row("shortName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val FloatValue(lat) = row("lat").getOrElse("")
      val FloatValue(lng) = row("lng").getOrElse("")
      val addr = row("addr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val LongValue(userNum) = row("userNum").getOrElse("")
      val region = row("region").map {
        case StringValue(str) => str
        case _ => "南京"
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse("")
      College(collegeId, name, eName, shortName, imgUrl, lat, lng, addr, userNum, region,sortNo)
    }
  }

}

