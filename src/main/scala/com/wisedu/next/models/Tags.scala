package com.wisedu.next.models

import java.util.TimeZone

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class Tag(tagId: String, tagName: String, tagType: Int, tagLevel: Int, tagStatus: Int, isDisplay: Int,
               cTime: DateTime, mTime: DateTime, feedMTime: DateTime, description: String, extension: String,
               synonymousRelation: String, feedNum: Long, tagDomain: Int)

abstract class ConcreteTags(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def getById(tagId: String): Future[Option[Tag]] = {
    val query = "select * from tags where tagId = '" + tagId + "' limit 1"
    client.select[Tag](query) { row =>
      val tagId = row("tagId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tagName = row("tagName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(tagType) = row("tagType").getOrElse("")
      val IntValue(tagLevel) = row("tagLevel").getOrElse("")
      val IntValue(tagStatus) = row("tagStatus").getOrElse("")
      val IntValue(isDisplay) = row("isDisplay").getOrElse("")
      val timeValue(cTime) = row("cTime").getOrElse("")
      val timeValue(mTime) = row("mTime").getOrElse("")
      val timeValue(feedMTime) = row("feedMTime").getOrElse("")
      val description = row("description").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val extension = row("extension").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val synonymousRelation = row("synonymousRelation").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val LongValue(feedNum) = row("feedNum").getOrElse("")
      val IntValue(tagDomain) = row("tagDomain").getOrElse("")
      Tag(tagId, tagName, tagType, tagLevel, tagStatus, isDisplay,
        new DateTime(cTime), new DateTime(mTime), new DateTime(feedMTime),
        description, extension, synonymousRelation, feedNum,tagDomain)
    }.map(_.headOption)
  }

  def delById(tagId: String): Future[Result] = {
    val delSql = "delete from tags where tagId = '" + tagId + "'"
    client.query(delSql)
  }

  def insTag(tag: Tag): Future[Result] = {
    val insertSql =
      """INSERT INTO tags (tagId, tagName, tagType, tagLevel, tagStatus, isDisplay, cTime,
        mTime, feedMTime, description, extension, synonymousRelation, feedNum ,tagDomain) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,?)"""
    val ps = client.prepare(insertSql)
    ps(tag.tagId, tag.tagName, tag.tagType, tag.tagLevel, tag.tagStatus, tag.isDisplay, tag.cTime.toDate,
      tag.mTime.toDate, tag.feedMTime.toDate, tag.description, tag.extension, tag.synonymousRelation, tag.feedNum, tag.tagDomain)
  }

  def updTag(tag: Tag): Future[Result] = {
    val updSql =
      """update tags set tagName = ?, tagType = ?, tagLevel = ?, tagStatus = ?, isDisplay = ?,
        mTime = ?, description = ?, extension = ?, synonymousRelation = ? ,tagDomain = ? where tagId = ?"""
    val ps = client.prepare(updSql)
    ps( tag.tagName, tag.tagType, tag.tagLevel, tag.tagStatus, tag.isDisplay,
      tag.mTime.toDate, tag.description, tag.extension, tag.synonymousRelation, tag.tagDomain,tag.tagId)
  }


  def getByName(tagName: String): Future[Option[Boolean]] = {
    val query = "select tagId from tags where tagName = '" + tagName + "' limit 1"
    client.select[Boolean](query) { row =>
      row("tagId") match {
        case Some(item) => true
        case None => false
      }
    }.map(_.headOption)
  }


  def collDomainEnabledTag(tagDomain: Int): Future[Seq[Tag]] = {
    val query = "select * from tags  where tagDomain = " + tagDomain + " and tagStatus = 1 order by mTime desc "
    client.select[Tag](query) { row =>
     val tagId = row("tagId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tagName = row("tagName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(tagType) = row("tagType").getOrElse("")
      val IntValue(tagLevel) = row("tagLevel").getOrElse("")
      val IntValue(tagStatus) = row("tagStatus").getOrElse("")
      val IntValue(isDisplay) = row("isDisplay").getOrElse("")
      val timeValue(cTime) = row("cTime").getOrElse("")
      val timeValue(mTime) = row("mTime").getOrElse("")
      val timeValue(feedMTime) = row("feedMTime").getOrElse("")
      val description = row("description").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val extension = row("extension").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val synonymousRelation = row("synonymousRelation").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val LongValue(feedNum) = row("feedNum").getOrElse("")
      val IntValue(tagDomain) = row("tagDomain").getOrElse("")
      Tag(tagId, tagName, tagType, tagLevel, tagStatus, isDisplay,
        new DateTime(cTime), new DateTime(mTime), new DateTime(feedMTime),
        description, extension, synonymousRelation, feedNum,tagDomain)
    }
  }
  def collTags(tagDomain: Int, limit: Int, offset: Int): Future[Seq[Tag]] = {
    val query = "select * from tags  where tagDomain = " + tagDomain + " order by mTime desc limit  " + offset + "," + limit
    client.select[Tag](query) { row =>
     val tagId = row("tagId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val tagName = row("tagName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(tagType) = row("tagType").getOrElse("")
      val IntValue(tagLevel) = row("tagLevel").getOrElse("")
      val IntValue(tagStatus) = row("tagStatus").getOrElse("")
      val IntValue(isDisplay) = row("isDisplay").getOrElse("")
      val timeValue(cTime) = row("cTime").getOrElse("")
      val timeValue(mTime) = row("mTime").getOrElse("")
      val timeValue(feedMTime) = row("feedMTime").getOrElse("")
      val description = row("description").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val extension = row("extension").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val synonymousRelation = row("synonymousRelation").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val LongValue(feedNum) = row("feedNum").getOrElse("")
      val IntValue(tagDomain) = row("tagDomain").getOrElse("")
      Tag(tagId, tagName, tagType, tagLevel, tagStatus, isDisplay,
        new DateTime(cTime), new DateTime(mTime), new DateTime(feedMTime),
        description, extension, synonymousRelation, feedNum,tagDomain)
    }
  }

  def collTagSize(tagDomain: Int): Future[Int] = {
    val query = "SELECT count(1) as value from  tags  where tagDomain = " + tagDomain

    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }

}
