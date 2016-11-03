package com.wisedu.next.models

import _root_.java.lang.{Long => JLong}

import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.util.Future
import com.wisedu.next.consts.SysCodeKeys
import com.wisedu.next.services.BaseFunctions
import com.wisedu.next.types.NextTypes.MysqlClient

case class SysCode(value: String, display: String, typeId: String, typeName: String, sortNo: Int, description: String)

abstract class ConcreteSysCodes(val client: MysqlClient, val redisClient: Client) {

  def getByIdWithCache(typeId: String, value: String): Future[Option[SysCode]] = {
    val key = SysCodeKeys.SysCodeTypeIdAndValue.format(typeId, value)
    redisClient.get(StringToChannelBuffer(key)).map {
      result => result.map(item => BaseFunctions.channelBuffer2Object[SysCode](item))
    }
  }

  def delByIdWithCache(typeId: String, value: String): Future[JLong] = {
    val key = SysCodeKeys.SysCodeTypeIdAndValue.format(typeId, value)
    redisClient.del(Seq(StringToChannelBuffer(key)))
  }

  def setByIdWithCache(sysCode: SysCode): Future[Unit] = {
    val key = SysCodeKeys.SysCodeTypeIdAndValue.format(sysCode.typeId, sysCode.value)
    redisClient.setEx(StringToChannelBuffer(key), 60 * 24 * 7, BaseFunctions.object2ChannelBuffer(sysCode))
  }

  def getByTypeIdWithCache(typeId: String): Future[Seq[SysCode]] = {
    val key = SysCodeKeys.SysCodeTypeId.format(typeId)
    redisClient.get(StringToChannelBuffer(key)).map {
      case Some(result) => BaseFunctions.channelBuffer2Object[Seq[SysCode]](result)
      case None => Seq()
    }
  }

  def setByTypeIdWithCache(typeId: String, sysCodes: Seq[SysCode]): Future[Unit] = {
    val key = SysCodeKeys.SysCodeTypeId.format(typeId)
    redisClient.setEx(StringToChannelBuffer(key), 60 * 24 * 7, BaseFunctions.object2ChannelBuffer(sysCodes))
  }

  def delByTypeIdWithCache(typeId: String): Future[JLong] = {
    val key = SysCodeKeys.SysCodeTypeId.format(typeId)
    redisClient.del(Seq(StringToChannelBuffer(key)))
  }


  def getById(typeId: String, value: String): Future[Option[SysCode]] = {
    val query = "SELECT * FROM sysCodes WHERE typeId = '" + typeId + "' and value = '" + value + "'   limit 1 "
    client.select[SysCode](query) { row =>
      val value = row("value").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val display = row("display").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val typeId = row("typeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val typeName = row("typeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse("0")
      val description = row("description").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      SysCode(value, display, typeId, typeName,sortNo,description)
    }.map(_.headOption)
  }

  def insSysCode(sysCode: SysCode): Future[Result] = {

    getById("system_type", sysCode.typeId).map {
      case Some(item) => item.display
      case None => sysCode.typeId
    }.flatMap {
      dis => {
        val insertSql =
          """INSERT INTO sysCodes (value, display,typeId,typeName,sortNo,description
                    ) VALUES (?, ?, ?,? , ?, ? )"""
        client.prepare(insertSql)(sysCode.value, sysCode.display, sysCode.typeId, dis, sysCode.sortNo,
          sysCode.description)
      }
    }

  }

  def updateSysCode(sysCode: SysCode): Future[Result] = {
    val updSql =
      """update sysCodes set display = ?,sortNo = ?,description = ? where typeId = ? and value = ?
      """
    client.prepare(updSql)(sysCode.display, sysCode.sortNo,
      sysCode.description, sysCode.typeId, sysCode.value)
  }

  def delSysCode(typeId: String, value: String): Future[Result] = {
    val delSql =
      """delete from sysCodes where typeId = ? and value = ? """
    client.prepare(delSql)(typeId, value)
  }

  def getByTypeId(typeId: String): Future[Seq[SysCode]] = {
    val query = "SELECT * FROM sysCodes WHERE typeId = '" + typeId + "' order by sortNo "
    client.select[SysCode](query) { row =>
      val value = row("value").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val display= row("display").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val typeId = row("typeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val typeName = row("typeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse("0")
      val description = row("description").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      SysCode(value, display, typeId, typeName,sortNo,description)
    }
  }

  def collSysCodes(typeId: String, limit: Int, offSet: Int): Future[Seq[SysCode]] = {
    var query = "SELECT * FROM sysCodes "
    if (!typeId.isEmpty) {
      query += "WHERE  typeId = '" + typeId + "' "
    }
    query += " order by sortNo  limit " + offSet + "," + limit
    client.select[SysCode](query) { row =>
      val value= row("value").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val display = row("display").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val typeId = row("typeId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val typeName = row("typeName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
      val description = row("description").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      SysCode(value, display, typeId, typeName,sortNo,description)
    }
  }

  def collSysCodesSize(typeId: String): Future[Int] = {
    var query = "SELECT count(1) as value FROM sysCodes "
    if (!typeId.isEmpty) {
      query += "WHERE  typeId = '" + typeId + "' "
    }
    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse(0)
      value.toInt
    }.map(_.head)
  }
}