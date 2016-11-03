package com.wisedu.next.models

import _root_.java.lang.{Long => JLong}
import java.util.TimeZone

import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.util.Future
import com.wisedu.next.consts.CollegeKeys
import com.wisedu.next.services.BaseFunctions
import com.wisedu.next.types.NextTypes.MysqlClient

case class Depart(departId: String, collegeId: String, departName: String, descr: String, sortNo: Int, departShortName: String,mobileId:String)

abstract class ConcreteDeparts(val client: MysqlClient, val redisClient: Client) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def insDepart(depart: Depart): Future[Result] = {
    val sql = "insert into departs(departId,collegeId,departName,descr,sortNo,departShortName) values(?,?,?,?,?,?)"
    client.prepare(sql)(depart.departId, depart.collegeId, depart.departName, depart.descr, depart.sortNo, depart.departShortName)
  }

  def updDepart(depart: Depart): Future[Result] = {
    val sql = "update  departs set departName = ?,descr = ?,sortNo = ?,departShortName = ? where  departId = ?"
    client.prepare(sql)(depart.departName, depart.descr, depart.sortNo, depart.departShortName, depart.departId)
  }

  def delDepart(departId: String): Future[Result] = {
    val sql = "delete from departs  where  departId = ?"
    client.prepare(sql)(departId)
  }

  def getDepartById(departId: String): Future[Option[Depart]] = {
    val sql = "select * from departs where departId = ? limit 1"
    client.prepare(sql).select[Depart](departId){row =>
      val departId = row("departId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val departName = row("departName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val departShortName = row("departShortName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
      val mobileId = row("mobileId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      Depart(departId,collegeId,departName,descr,sortNo,departShortName,mobileId)
    }.map(_.headOption)
  }

  def getDepartsByCollegeIdWithCache(collegeId: String): Future[Seq[Depart]] = {
    val key = CollegeKeys.DepartsCollegeId.format(collegeId)
    redisClient.get(StringToChannelBuffer(key)).map {
      case Some(result) => BaseFunctions.channelBuffer2Object[Seq[Depart]](result)
      case None => Seq()
    }
  }

  def setDepartsByCollegeIdWithCache(collegeId: String, departs: Seq[Depart]): Future[Unit] = {
    val key = CollegeKeys.DepartsCollegeId.format(collegeId)
    redisClient.setEx(StringToChannelBuffer(key), 60 * 24 * 7, BaseFunctions.object2ChannelBuffer(departs))
  }

  def delDepartsByCollegeIdWithCache(collegeId: String): Future[JLong] = {
    val key = CollegeKeys.DepartsCollegeId.format(collegeId)
    redisClient.del(Seq(StringToChannelBuffer(key)))
  }


  def getDepartsByCollegeId(collegeId: String): Future[Seq[Depart]] = {
    val sql = "select * from departs where collegeId = ? order by sortNo "
    client.prepare(sql).select[Depart](collegeId){row =>
      val departId = row("departId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val departName = row("departName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val departShortName = row("departShortName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
      val mobileId = row("mobileId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      Depart(departId,collegeId,departName,descr,sortNo,departShortName,mobileId)
    }
  }

  def getDepartsByCollegeIdAndMId(collegeId: String,mobileId:String): Future[Option[Depart]] = {
    val sql = "select * from departs where collegeId = ? and mobileId = ? limit 1 "
    client.prepare(sql).select[Depart](collegeId,mobileId){row =>
      val departId = row("departId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val departName = row("departName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val departShortName = row("departShortName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
      val mobileId = row("mobileId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      Depart(departId,collegeId,departName,descr,sortNo,departShortName,mobileId)
    }.map(_.headOption)
  }

  def getDepartsByName(departName: String): Future[Option[Depart]] = {
    val sql = "select * from departs where departName = ? limit 1 "
    client.prepare(sql).select[Depart](departName){row =>
      val departId = row("departId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val departName = row("departName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val descr = row("descr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val departShortName = row("departShortName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
      val mobileId = row("mobileId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      Depart(departId,collegeId,departName,descr,sortNo,departShortName,mobileId)
    }.map(_.headOption)
  }

}
