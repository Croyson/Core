package com.wisedu.next.models

import java.util.TimeZone

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class Service(serId: String, name: String, orgType: Int, srcId: String, srcType: Int, imgUrl: String,
                   backImgUrl: String, depict: String, cTime: DateTime, mTime: DateTime, collegeId: String,
                   sortNo:Int,isDisplay:Int)

abstract class ConcreteServices(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getById(serId: String): Future[Option[Service]] = {
    val query = "SELECT * FROM services WHERE serId = '" + serId + "' limit 1 "
    client.select[Service](query) { row =>
      val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(orgType) = row("orgType").getOrElse("")
      val srcId = row("srcId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(srcType) = row("srcType").getOrElse("")
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val backImgUrl = row("backImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val depict = row("depict").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(mTime) = row("mTime").getOrElse("0000-00-00 00:00:00")
      val IntValue(sortNo) = row("sortNo").getOrElse("")
      val IntValue(isDisplay ) = row("isDisplay").getOrElse("")
      Service(serId, name, orgType, srcId, srcType, imgUrl, backImgUrl, depict,
        new DateTime(cTime), new DateTime(mTime),collegeId,sortNo,isDisplay)
    }.map(_.headOption)
  }

  def collServices(): Future[Seq[Service]] = {
    val query = "SELECT * FROM services order by collegeId"
    client.select[Service](query) { row =>
      val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(orgType) = row("orgType").getOrElse("")
      val srcId = row("srcId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(srcType) = row("srcType").getOrElse("")
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val backImgUrl = row("backImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depict = row("depict").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(mTime) = row("mTime").getOrElse("0000-00-00 00:00:00")
      val IntValue(sortNo) = row("sortNo").getOrElse("")
      val IntValue(isDisplay ) = row("isDisplay").getOrElse("")
      Service(serId, name, orgType, srcId, srcType, imgUrl, backImgUrl, depict,
        new DateTime(cTime), new DateTime(mTime),collegeId,sortNo,isDisplay)
    }
  }

  def collServices(limit: Int, offset: Int): Future[Seq[Service]] = {
    val query = "SELECT * FROM services order by sortNo desc,mTime desc  limit " + offset + "," + (limit + offset)
    client.select[Service](query) { row =>
      val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(orgType) = row("orgType").getOrElse("")
      val srcId = row("srcId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(srcType) = row("srcType").getOrElse("")
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val backImgUrl = row("backImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depict = row("depict").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(mTime) = row("mTime").getOrElse("0000-00-00 00:00:00")
       val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse("")
       val IntValue(isDisplay ) = row("isDisplay").getOrElse("")
      Service(serId, name, orgType, srcId, srcType, imgUrl, backImgUrl, depict,
        new DateTime(cTime), new DateTime(mTime),collegeId,sortNo,isDisplay)
    }
  }


  def delById(serId: String): Future[Result] = {
    val delSql = "delete from services where serId = '" + serId + "'"
    client.query(delSql)
  }

  def insService(service: Service): Future[Result] = {
    val insertSQL =
      """INSERT INTO services (serId, name, orgType, srcId, srcType,imgUrl,backImgUrl,depict,cTime,
                      mTime,collegeId,sortNo,isDisplay) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?)"""
    client.prepare(insertSQL)(service.serId.toString, service.name, service.orgType, service.srcId, service.srcType, service.imgUrl,
      service.backImgUrl, service.depict, service.cTime.toDate, service.mTime.toDate, service.collegeId,service.sortNo,
    service.isDisplay)
  }

  def updService(service: Service): Future[Result] = {
    val updSQL =
      """update services set name = ?, orgType = ?, srcId = ?, srcType = ?,imgUrl = ?,backImgUrl = ?,depict = ?,
                     collegeId = ?,sortNo = ?,isDisplay = ?  where  serId = ? """
    client.prepare(updSQL)(service.name, service.orgType, service.srcId, service.srcType, service.imgUrl,
      service.backImgUrl, service.depict, service.collegeId,service.sortNo,service.isDisplay,service.serId.toString)
  }

  def updServiceMTime(serId:String,mTime:DateTime): Future[Result] = {
    val sql = "update services set mTime = ? where  serId = ?"
    client.prepare(sql)(mTime.toDate,serId)
  }

  def collServices(name: String, srcType: String, orgType: String, limit: Int, offSet: Int): Future[Seq[Service]] = {
    var query = "SELECT * FROM services where 1 = 1"
    if (!name.isEmpty) {
      query += " and name like '%" + name + "%'"
    }
    if (!srcType.isEmpty) {
      query += " and srcType = '" + srcType + "'"
    }
    if (!orgType.isEmpty) {
      query += " and orgType = '" + orgType + "'"
    }
    query += " order by sortNo desc,mtime desc  limit " + offSet + "," + limit
    client.select[Service](query) { row =>
     val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(orgType) = row("orgType").getOrElse("")
      val srcId = row("srcId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(srcType) = row("srcType").getOrElse("")
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val backImgUrl = row("backImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depict = row("depict").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(mTime) = row("mTime").getOrElse("0000-00-00 00:00:00")
       val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse("")
      val IntValue(isDisplay ) = row("isDisplay").getOrElse("")
      Service(serId, name, orgType, srcId, srcType, imgUrl, backImgUrl, depict,
        new DateTime(cTime), new DateTime(mTime),collegeId,sortNo,isDisplay)
    }
  }

  def collServicesSize(name: String, srcType: String, orgType: String): Future[Int] = {
    var query = "SELECT count(1) as value FROM services where 1 = 1"
    if (!name.isEmpty) {
      query += " and name like '%" + name + "%'"
    }
    if (!srcType.isEmpty) {
      query += " and srcType = '" + srcType + "'"
    }
    if (!orgType.isEmpty) {
      query += " and orgType = '" + orgType + "'"
    }
    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }

  def collServicesByCollegeId(college_id: String, filterType: String, limit: Int, offSet: Int): Future[Seq[Service]] = {
    var query = "SELECT * FROM services where  isDisplay = 1 "

    if (filterType == "relative") {
      query += " and collegeId = '" + college_id + "'"
    }

    if (filterType == "unrelative") {
      query += " and collegeId != '" + college_id + "'"
    }

    query += " order by sortNo desc,mtime desc  limit " + offSet + "," + limit
    client.select[Service](query) { row =>
      val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val name = row("name").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(orgType) = row("orgType").getOrElse("")
      val srcId = row("srcId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(srcType) = row("srcType").getOrElse("")
      val imgUrl = row("imgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val backImgUrl = row("backImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val depict = row("depict").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(mTime) = row("mTime").getOrElse("0000-00-00 00:00:00")
      val collegeId = row("collegeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse("")
      val IntValue(isDisplay ) = row("isDisplay").getOrElse("")
      Service(serId, name, orgType, srcId, srcType, imgUrl, backImgUrl, depict,
        new DateTime(cTime), new DateTime(mTime),collegeId,sortNo,isDisplay)
    }
  }

}
