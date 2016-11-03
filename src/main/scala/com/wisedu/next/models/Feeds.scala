package com.wisedu.next.models

import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.websudos.phantom.dsl.{Row, _}
import com.wisedu.next.types.NextTypes.MysqlClient
import org.joda.time.DateTime

case class Feed(feedId: UUID, title: String, serId: String, srcType: Int, viewStyle: String, summ: String,
                content: String, contUrl: String, source: String, srcUrl: String, videoShareCode: String, videoAddr: String,
                sImgUrl: String, status: Int, cTime: DateTime, mTime: DateTime, gTime: DateTime,
                resUrl: String, liveStartTime: DateTime, liveStatus: Int, permitUpdate: Int,
                screenPopUpStatus: Int, updateType: String,
                displayChannel: String, videoLength: String, lotteryDrawId: String, videoType: Int, group1: String,
                group2: String, group3: String, group4: String, group5: String, group6: String, contentType: Int, sortNo: Int,
                topicInviter: String, listImgType: Int, advImg: String, permitEmotions: Int, permitThumbsUp: Int)

case class SqlFeed(feedId: UUID, title: String, serId: String, srcType: Int, viewStyle: String, summ: String,
                   contUrl: String, srcUrl: String, videoShareCode: String, videoAddr: String,
                   sImgUrl: String, status: Int, cTime: DateTime, mTime: DateTime, gTime: DateTime,
                   resUrl: String, liveStartTime: DateTime, liveStatus: Int, permitUpdate: Int,
                   screenPopUpStatus: Int, updateType: String,
                   displayChannel: String, videoLength: String, lotteryDrawId: String, videoType: Int, group1: String,
                   group2: String, group3: String, group4: String, group5: String, group6: String, contentType: Int, sortNo: Int,
                   topicInviter: String, listImgType: Int, advImg: String, permitEmotions: Int, permitThumbsUp: Int)

class Feeds extends CassandraTable[Feeds, Feed] {

  object feedId extends UUIDColumn(this) with PrimaryKey[UUID]

  object title extends StringColumn(this)

  object serId extends StringColumn(this)

  object srcType extends IntColumn(this)

  object viewStyle extends StringColumn(this)

  object summ extends StringColumn(this)

  object content extends StringColumn(this)

  object contUrl extends StringColumn(this)

  object source extends StringColumn(this)

  object srcUrl extends StringColumn(this)

  object videoShareCode extends StringColumn(this)

  object videoAddr extends StringColumn(this)

  object sImgUrl extends StringColumn(this)


  object status extends IntColumn(this)

  object cTime extends DateTimeColumn(this)

  object mTime extends DateTimeColumn(this)

  object gTime extends DateTimeColumn(this)

  object sdTime extends DateTimeColumn(this)

  object resUrl extends StringColumn(this)

  object liveStartTime extends DateTimeColumn(this)

  object liveStatus extends IntColumn(this)

  object permitUpdate extends IntColumn(this)

  object screenPopUpStatus extends IntColumn(this)

  object updateType extends StringColumn(this)

  object displayChannel extends StringColumn(this)

  object videoLength extends StringColumn(this)

  object lotteryDrawId extends StringColumn(this)

  object videoType extends IntColumn(this)

  object group1 extends StringColumn(this)

  object group2 extends StringColumn(this)

  object group3 extends StringColumn(this)

  object group4 extends StringColumn(this)

  object group5 extends StringColumn(this)

  object group6 extends StringColumn(this)

  object contentType extends IntColumn(this)

  object sortNo extends IntColumn(this)

  object topicInviter extends StringColumn(this)

  object advImg extends StringColumn(this)

  object listImgType extends IntColumn(this)

  object permitEmotions extends IntColumn(this)

  object permitThumbsUp extends IntColumn(this)


  def fromRow(row: Row): Feed = {
    Feed(feedId(row), title(row), serId(row), srcType(row), viewStyle(row), summ(row), content(row), contUrl(row),
      source(row), srcUrl(row), videoShareCode(row), videoAddr(row), sImgUrl(row), status(row),
      cTime(row), mTime(row), gTime(row), resUrl(row), liveStartTime(row), liveStatus(row),
      permitUpdate(row), screenPopUpStatus(row), updateType(row),
      displayChannel(row), videoLength(row), lotteryDrawId(row), videoType(row), group1(row),
      group2(row), group3(row), group4(row), group5(row), group6(row), contentType(row), sortNo(row),
      topicInviter(row), listImgType(row), advImg(row), permitEmotions(row), permitThumbsUp(row))
  }
}

abstract class ConcreteFeeds(val client: MysqlClient) extends Feeds with RootConnector {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def toSqlFeed(feed: Feed): SqlFeed = {
    SqlFeed(feed.feedId, feed.title, feed.serId, feed.srcType, feed.viewStyle, feed.summ, feed.contUrl,
      feed.srcUrl, feed.videoShareCode, feed.videoAddr, feed.sImgUrl, feed.status, feed.cTime,
      feed.mTime, feed.gTime, feed.resUrl, feed.liveStartTime, feed.liveStatus, feed.permitUpdate,
      feed.screenPopUpStatus, feed.updateType, feed.displayChannel,
      feed.videoLength, feed.lotteryDrawId, feed.videoType, feed.group1, feed.group2, feed.group3, feed.group4, feed.group5,
      feed.group6, feed.contentType, feed.sortNo, feed.topicInviter, feed.listImgType, feed.advImg, feed.permitEmotions,
      feed.permitThumbsUp)
  }

  def toFeed(sqlFeed: SqlFeed): Feed = {
    Feed(sqlFeed.feedId, sqlFeed.title, sqlFeed.serId, sqlFeed.srcType, sqlFeed.viewStyle, sqlFeed.summ, "", sqlFeed.contUrl,
      "", sqlFeed.srcUrl, sqlFeed.videoShareCode, sqlFeed.videoAddr, sqlFeed.sImgUrl, sqlFeed.status, sqlFeed.cTime,
      sqlFeed.mTime, sqlFeed.gTime, sqlFeed.resUrl, sqlFeed.liveStartTime, sqlFeed.liveStatus, sqlFeed.permitUpdate,
      sqlFeed.screenPopUpStatus, sqlFeed.updateType, sqlFeed.displayChannel,
      sqlFeed.videoLength, sqlFeed.lotteryDrawId, sqlFeed.videoType, sqlFeed.group1, sqlFeed.group2, sqlFeed.group3, sqlFeed.group4,
      sqlFeed.group5, sqlFeed.group6, sqlFeed.contentType, sqlFeed.sortNo, sqlFeed.topicInviter, sqlFeed.listImgType, sqlFeed.advImg
      , sqlFeed.permitEmotions, sqlFeed.permitThumbsUp)
  }


  def insFeed(feed: Feed): Future[Result] = {
    insert.value(_.feedId, feed.feedId).value(_.title, feed.title).value(_.serId, feed.serId)
      .value(_.srcType, feed.srcType).value(_.viewStyle, feed.viewStyle).value(_.summ, feed.summ)
      .value(_.content, feed.content).value(_.contUrl, feed.contUrl).value(_.source, feed.source)
      .value(_.srcUrl, feed.srcUrl).value(_.videoShareCode, feed.videoShareCode).value(_.videoAddr, feed.videoAddr)
      .value(_.sImgUrl, feed.sImgUrl).value(_.status, feed.status)
      .value(_.cTime, feed.cTime).value(_.mTime, feed.mTime).value(_.gTime, feed.gTime)
      .value(_.resUrl, feed.resUrl).value(_.liveStartTime, feed.liveStartTime)
      .value(_.liveStatus, feed.liveStatus).value(_.permitUpdate, feed.permitUpdate)
      .value(_.screenPopUpStatus, feed.screenPopUpStatus)
      .value(_.updateType, feed.updateType)
      .value(_.lotteryDrawId, feed.lotteryDrawId)
      .value(_.videoType, feed.videoType)
      .value(_.group1, feed.group1)
      .value(_.group2, feed.group2)
      .value(_.group3, feed.group3)
      .value(_.group4, feed.group4)
      .value(_.group5, feed.group5)
      .value(_.group6, feed.group6)
      .value(_.contentType, feed.contentType)
      .value(_.sortNo, feed.sortNo)
      .value(_.topicInviter, feed.topicInviter)
      .value(_.displayChannel, feed.displayChannel)
      .value(_.listImgType, feed.listImgType)
      .value(_.advImg, feed.advImg)
      .value(_.permitEmotions, feed.permitEmotions)
      .value(_.permitThumbsUp, feed.permitThumbsUp)
      .value(_.videoLength, feed.videoLength).execute().flatMap {
      rst => {
        val insertSql =
          """INSERT INTO feeds (feedId, title, serId, srcType, viewStyle, summ, contUrl, srcUrl, videoShareCode,
      videoAddr, sImgUrl, status, cTime, mTime, gTime, resUrl, liveStartTime, liveStatus, permitUpdate,
       screenPopUpStatus, updateType, displayChannel, videoLength,lotteryDrawId,
      videoType,group1,group2,group3,group4,group5,group6,contentType,sortNo,eTime,topicInviter,advImg,
      listImgType,permitEmotions,permitThumbsUp)
      VALUES (?, ?, ?, ?, ?, ?, ?,?,?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?,?,?,now(),?,?,?)"""
        val ps = client.prepare(insertSql)
        val sqlFeed = toSqlFeed(feed)
        ps(sqlFeed.feedId.toString, sqlFeed.title, sqlFeed.serId.toString, sqlFeed.srcType, sqlFeed.viewStyle,
          sqlFeed.summ, sqlFeed.contUrl, sqlFeed.srcUrl, sqlFeed.videoShareCode, sqlFeed.videoAddr, sqlFeed.sImgUrl,
          sqlFeed.status, sqlFeed.cTime.toDate, sqlFeed.mTime.toDate, sqlFeed.gTime.toDate,
          sqlFeed.resUrl, sqlFeed.liveStartTime.toDate, sqlFeed.liveStatus,
          sqlFeed.permitUpdate,
          sqlFeed.screenPopUpStatus, sqlFeed.updateType, sqlFeed.displayChannel,
          sqlFeed.videoLength, sqlFeed.lotteryDrawId, sqlFeed.videoType, sqlFeed.group1,
          sqlFeed.group2, sqlFeed.group3, sqlFeed.group4, sqlFeed.group5, sqlFeed.group6,
          sqlFeed.contentType, sqlFeed.sortNo, sqlFeed.topicInviter, sqlFeed.advImg, sqlFeed.listImgType,
          sqlFeed.permitEmotions, sqlFeed.permitThumbsUp)
      }
    }
  }

  def delFeed(feedId: UUID): Future[Result] = {
    delete.where(_.feedId eqs feedId).execute().flatMap {
      rst => {
        val delSql = "delete from feeds where feedId = '" + feedId.toString + "'"
        client.query(delSql)
      }
    }
  }

  def updFeed(feed: Feed): Future[Result] = {


    update.where(_.feedId eqs feed.feedId).modify(_.title setTo feed.title).and(_.serId setTo feed.serId)
      .and(_.srcType setTo feed.srcType).and(_.viewStyle setTo feed.viewStyle).and(_.summ setTo feed.summ)
      .and(_.contUrl setTo feed.contUrl)
      .and(_.srcUrl setTo feed.srcUrl)
      .and(_.videoShareCode setTo feed.videoShareCode).and(_.videoAddr setTo feed.videoAddr)
      .and(_.sImgUrl setTo feed.sImgUrl)
      .and(_.mTime setTo feed.mTime)
      .and(_.liveStartTime setTo feed.liveStartTime)
      .and(_.liveStatus setTo feed.liveStatus).and(_.permitUpdate setTo feed.permitUpdate)
      .and(_.screenPopUpStatus setTo feed.screenPopUpStatus)
      .and(_.updateType setTo feed.updateType)
      .and(_.lotteryDrawId setTo feed.lotteryDrawId)
      .and(_.displayChannel setTo feed.displayChannel)
      .and(_.videoType setTo feed.videoType)
      .and(_.group1 setTo feed.group1)
      .and(_.group2 setTo feed.group2)
      .and(_.group3 setTo feed.group3)
      .and(_.group4 setTo feed.group4)
      .and(_.group5 setTo feed.group5)
      .and(_.group6 setTo feed.group6)
      .and(_.contentType setTo feed.contentType)
      .and(_.sortNo setTo feed.sortNo)
      .and(_.topicInviter setTo feed.topicInviter)
      .and(_.listImgType setTo feed.listImgType)
      .and(_.advImg setTo feed.advImg)
      .and(_.permitEmotions setTo feed.permitEmotions)
      .and(_.permitThumbsUp setTo feed.permitThumbsUp)
      .and(_.videoLength setTo feed.videoLength).execute().flatMap {
      rst => {

        if (1 == feed.contentType) {
          update.where(_.feedId eqs feed.feedId).
            modify(_.content setTo feed.content).
            and(_.source setTo feed.source).
            and(_.resUrl setTo feed.resUrl).execute()
        }

        val updSql =
          """update feeds set title = ?, serId = ?, srcType = ?, viewStyle = ?, summ = ?, contUrl = ?,
        videoShareCode = ?, videoAddr = ?, sImgUrl = ?, mTime = ?,
        srcUrl = ?, liveStartTime = ?, liveStatus = ?, permitUpdate = ?, screenPopUpStatus = ?,
      updateType = ?, displayChannel = ?, videoLength = ?,lotteryDrawId = ?,
      videoType = ?,group1 = ?,group2 = ?,group3 = ?,group4 = ?,group5 = ?,group6 = ?,contentType = ?,sortNo = ?,
      eTime=now(),topicInviter=?,advImg=?,listImgType=?,permitEmotions=?,permitThumbsUp=?
      where feedId = ?"""
        val ps = client.prepare(updSql)
        val sqlFeed = toSqlFeed(feed)
        ps(sqlFeed.title, sqlFeed.serId.toString, sqlFeed.srcType, sqlFeed.viewStyle,
          sqlFeed.summ, sqlFeed.contUrl, sqlFeed.videoShareCode, sqlFeed.videoAddr, sqlFeed.sImgUrl,
          sqlFeed.mTime.toDate,
          sqlFeed.srcUrl, sqlFeed.liveStartTime.toDate, sqlFeed.liveStatus,
          sqlFeed.permitUpdate,
          sqlFeed.screenPopUpStatus, sqlFeed.updateType, sqlFeed.displayChannel, sqlFeed.videoLength,
          sqlFeed.lotteryDrawId, sqlFeed.videoType, sqlFeed.group1,
          sqlFeed.group2, sqlFeed.group3, sqlFeed.group4, sqlFeed.group5, sqlFeed.group6, sqlFeed.contentType,
          sqlFeed.sortNo, sqlFeed.topicInviter, sqlFeed.advImg, sqlFeed.listImgType, sqlFeed.permitEmotions,
          sqlFeed.permitThumbsUp,
          sqlFeed.feedId.toString)
      }
    }
  }

  def updSImg(feedId: UUID, sImgUrl: String): Future[Result] = {
    update.where(_.feedId eqs feedId).modify(_.sImgUrl setTo sImgUrl).execute().flatMap {
      rst => {
        val updSql = "update feeds set sImgUrl = ? where feedId = ?"
        val ps = client.prepare(updSql)
        ps(sImgUrl, feedId.toString)
      }
    }
  }

  def updLotteryDrawId(feedId: UUID, lotteryDrawId: String): Future[Result] = {
    update.where(_.feedId eqs feedId).modify(_.lotteryDrawId setTo lotteryDrawId).execute().flatMap {
      rst => {
        val updSql = "update feeds set lotteryDrawId = ? where feedId = ?"
        val ps = client.prepare(updSql)
        ps(lotteryDrawId, feedId.toString)
      }
    }
  }

  def updStatus(feedId: UUID, status: Int): Future[Result] = {
    update.where(_.feedId eqs feedId).modify(_.status setTo status).execute().flatMap {
      rst => {
        val updSql = "update feeds set status = ? where feedId = ?"
        val ps = client.prepare(updSql)
        ps(status, feedId.toString)
      }
    }

  }

  def updService(feedId: UUID, serId: String): Future[Result] = {
    update.where(_.feedId eqs feedId).modify(_.serId setTo serId).execute().flatMap {
      rst => {
        val updSql = "update feeds set serId = ? where feedId = ?"
        val ps = client.prepare(updSql)
        ps(serId.toString, feedId.toString)
      }
    }
  }

  def updGroups(feedId: UUID, group1: String, group2: String, group3: String, group4: String, group5: String, group6: String): Future[Result] = {
    update.where(_.feedId eqs feedId).
      modify(_.group1 setTo group1).and(_.group2 setTo group2).
      and(_.group3 setTo group3).and(_.group4 setTo group4).
      and(_.group5 setTo group5).and(_.group6 setTo group6).execute().flatMap {
      rst => {
        val updSql = "update feeds set group1 = ?,group2 = ? ,group3 = ?, group4 = ?, group5 = ? ,group6 = ? where feedId = ?"
        val ps = client.prepare(updSql)
        ps(group1, group2, group3, group4, group5, group6, feedId.toString)
      }
    }
  }


  def updCont(feedId: UUID, contUrl: String): Future[Result] = {
    update.where(_.feedId eqs feedId).modify(_.contUrl setTo contUrl).execute().flatMap {
      rst => {
        val updSql = "update feeds set contUrl = ? where feedId = ?"
        val ps = client.prepare(updSql)
        ps(contUrl, feedId.toString)
      }
    }

  }

  def getById(feedId: UUID): Future[Option[Feed]] = {
    select.where(_.feedId eqs feedId).get()
  }

  def collFeeds(viewStyle: String, title: String, source: String, groupId: String, groupStrutsType: String, tag: String, viewStyles: String, tags: String, state: String,
                orderInfo: String, limits: Int, offSet: Int): Future[Seq[SqlFeed]] = {
    var query =
      """SELECT f.feedId,
                        	f.title,
                        	f.serId,
                        	f.srcType,
                        	f.viewStyle,
                        	f.summ,
                        	f.content,
                        	f.contUrl,
                        	f.source,
                        	f.srcUrl,
                        	f.videoShareCode,
                        	f.videoAddr,
                        	f.sImgUrl,
                        	f.`status`,
                        	f.videoLength,
                        	f.displayChannel,
                        	f.updateType,
                        	f.screenPopUpStatus,
                        	f.permitUpdate,
                        	f.liveStatus,
                        	f.liveStartTime,
                        	f.resUrl,
                        	f.gTime,
                        	f.mTime,
                        	f.cTime,
                          f.lotteryDrawId,
                          f.videoType,
                        	f.group1,
                        	f.group2,
                        	f.group4,
                        	f.group3,
                        	f.group5,
                        	f.group6,
                          f.contentType,
                          f.sortNo,
                          f.topicInviter,
                          f.advImg,
                          f.listImgType,
                          f.permitEmotions,
                          f.permitThumbsUp,
                        	fs.readNum,
                        	fs.likeNum,
                        	fs.unLikeNum,
                        	fs.collectNum,
                        	fs.updateNum,
                        	fs.shareNum,
                        	fs.onlineNum,
                        	fs.voteNum,
                        	fs.imgNum
                        FROM feeds f LEFT OUTER JOIN feedStats fs ON f.feedId = fs.feedId
                        WHERE 1 = 1 """
    if (!title.isEmpty) {
      query += " and title like '%" + title + "%'"
    }

    if (!viewStyle.isEmpty) {
      query += " and  viewStyle = '" + viewStyle + "'"
    }

    if (!source.isEmpty) {
      query += " and serId = '" + source + "'"
    }

    if (!state.isEmpty) {
      query += " and status = '" + state + "'"
    }

    if (!tag.isEmpty) {
      query += " and   EXISTS  (select feedId from  feedTags ft where tagId = '" + tag + "' and f.feedId =  ft.feedId )"
    }

    if (!groupId.isEmpty) {
      query += " and viewStyle in  ('" + viewStyles.replace(",", "','") + "')"
      if ("0".equals(groupStrutsType)) {
        // 虚拟组的是通过 tag 查询内容
        if (tag.isEmpty) {
          query += " and EXISTS (select feedId from  feedTags ft where tagId  in ('" + tags.replace(",", "','") + "') and f.feedId =  ft.feedId) "
        }
      } else if ("1".equals(groupStrutsType)) {
        //虚拟组是通过 内容虚拟组 关系查询内容
        query += " and   EXISTS  (select feedId from groupFeeds gf where  groupId = '" + groupId + "' and auditStatus = 1 and f.feedId =  gf.feedId )  "
      }

    }

    if (!orderInfo.isEmpty) {
      query += " order by " + orderInfo
    } else {
      query += " order by sortNo desc,mTime desc "
    }

    query += "limit " + offSet + "," + limits

    client.select[SqlFeed](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val title = row("title").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(srcType) = row("srcType").getOrElse("0")
      val viewStyle = row("viewStyle").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val summ = row("summ").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val contUrl = row("contUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val srcUrl = row("srcUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoShareCode = row("videoShareCode").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoAddr = row("videoAddr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sImgUrl = row("sImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(status) = row("status").getOrElse("")
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(mTime) = row("mTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(gTime) = row("gTime").getOrElse("0000-00-00 00:00:00")
      val resUrl = row("resUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(liveStartTime) = row("liveStartTime").getOrElse("0000-00-00 00:00:00")
      val IntValue(liveStatus) = row("liveStatus").getOrElse("")
      val IntValue(permitUpdate) = row("permitUpdate").getOrElse("")
      val IntValue(screenPopUpStatus) = row("screenPopUpStatus").getOrElse("")
      val updateType = row("updateType").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(videoType) = row("videoType").getOrElse("")
      val displayChannel = row("displayChannel").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoLength = row("videoLength").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawId = row("lotteryDrawId").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val group1 = row("group1").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group2 = row("group2").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group3 = row("group3").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group4 = row("group4").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group5 = row("group5").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group6 = row("group6").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val topicInviter = row("topicInviter").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(contentType) = row("contentType").getOrElse(0)
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
       val advImg = row("advImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(listImgType) = row("listImgType").getOrElse(0)
      val IntValue(permitEmotions) = row("permitEmotions").getOrElse(1)
      val IntValue(permitThumbsUp) = row("permitThumbsUp").getOrElse(1)
      SqlFeed(UUID.fromString(feedId), title, serId, srcType, viewStyle, summ, contUrl, srcUrl,
        videoShareCode, videoAddr, sImgUrl, status, new DateTime(cTime), new DateTime(mTime), new DateTime(gTime),
        resUrl, new DateTime(liveStartTime), liveStatus, permitUpdate,
        screenPopUpStatus, updateType, displayChannel, videoLength, lotteryDrawId, videoType, group1, group2, group3,
        group4, group5, group6, contentType, sortNo, topicInviter,listImgType,advImg,permitEmotions,permitThumbsUp)
    }
  }

  def collFeedSize(viewStyle: String, title: String, source: String, groupId: String, groupStrutsType: String, tag: String, viewStyles: String, tags: String, state: String,
                   orderInfo: String): Future[Int] = {
    var query = "select count(1) as value from feeds f where 1 = 1 "
    if (!title.isEmpty) {
      query += " and title like '%" + title + "%'"
    }
    if (!viewStyle.isEmpty) {
      query += " and viewStyle = '" + viewStyle + "'"
    }

    if (!source.isEmpty) {
      query += " and serId = '" + source + "'"
    }

    if (!state.isEmpty) {
      query += " and status = '" + state + "'"
    }

    if (!tag.isEmpty) {
      query += " and feedId in (select feedId from  feedTags where tagId = '" + tag + "') "
    }

    if (!groupId.isEmpty) {
      query += " and viewStyle in  ('" + viewStyles.replace(",", "','") + "')"
      if ("0".equals(groupStrutsType)) {
        // 虚拟组的是通过 tag 查询内容
        if (tag.isEmpty) {
          query += " and EXISTS (select feedId from  feedTags ft where tagId  in ('" + tags.replace(",", "','") + "') and f.feedId =  ft.feedId ) "
        }
      } else if ("1".equals(groupStrutsType)) {
        //虚拟组是通过 内容虚拟组 关系查询内容
        query += " and   EXISTS  (select feedId from groupFeeds gf where groupId = '" + groupId + "' and auditStatus = 1 and f.feedId =  gf.feedId)  "
      }

    }


    client.select(query) { row =>
      val LongValue(value) = row("value").getOrElse("0")
      value.toInt
    }.map(_.head)
  }

  def getFeedsBySerId(serId: String, status: String, limit: Int, offset: Int): Future[Seq[SqlFeed]] = {
    var query = "SELECT * FROM feeds where serId = '" + serId + "'"

    if (!status.isEmpty) {
      query += " and status = '" + status + "'"
    }

    query += " order by sortNo desc,mTime desc limit " + offset + ", " + limit + ""

    client.select[SqlFeed](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val title = row("title").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(srcType) = row("srcType").getOrElse("0")
      val viewStyle = row("viewStyle").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val summ = row("summ").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val contUrl = row("contUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val srcUrl = row("srcUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoShareCode = row("videoShareCode").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoAddr = row("videoAddr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sImgUrl = row("sImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(status) = row("status").getOrElse("")
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(mTime) = row("mTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(gTime) = row("gTime").getOrElse("0000-00-00 00:00:00")
      val resUrl = row("resUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(liveStartTime) = row("liveStartTime").getOrElse("0000-00-00 00:00:00")
      val IntValue(liveStatus) = row("liveStatus").getOrElse("")
      val IntValue(permitUpdate) = row("permitUpdate").getOrElse("")
      val IntValue(screenPopUpStatus) = row("screenPopUpStatus").getOrElse("")
      val updateType = row("updateType").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(videoType) = row("videoType").getOrElse("")
      val displayChannel = row("displayChannel").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoLength = row("videoLength").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawId = row("lotteryDrawId").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val group1 = row("group1").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group2 = row("group2").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group3 = row("group3").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group4 = row("group4").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group5 = row("group5").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group6 = row("group6").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val topicInviter = row("topicInviter").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(contentType) = row("contentType").getOrElse(0)
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
       val advImg = row("advImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(listImgType) = row("listImgType").getOrElse(0)
      val IntValue(permitEmotions) = row("permitEmotions").getOrElse(1)
      val IntValue(permitThumbsUp) = row("permitThumbsUp").getOrElse(1)
      SqlFeed(UUID.fromString(feedId), title, serId, srcType, viewStyle, summ, contUrl, srcUrl,
        videoShareCode, videoAddr, sImgUrl, status, new DateTime(cTime), new DateTime(mTime), new DateTime(gTime),
        resUrl, new DateTime(liveStartTime), liveStatus, permitUpdate,
        screenPopUpStatus, updateType, displayChannel, videoLength, lotteryDrawId, videoType, group1, group2, group3,
        group4, group5, group6, contentType, sortNo, topicInviter,listImgType,advImg,permitEmotions,permitThumbsUp)
    }
  }

  def getFeedsByViewStyle(feedId: String, viewStyle: String): Future[Seq[SqlFeed]] = {
    val query = "select * from feeds where feedId in ('" + feedId.replaceAll(",", "','") + "') and viewStyle in ('" + viewStyle.replaceAll(",", "','") + "') order by sortNo desc,mTime desc"
    client.select[SqlFeed](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val title = row("title").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(srcType) = row("srcType").getOrElse("0")
      val viewStyle = row("viewStyle").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val summ = row("summ").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val contUrl = row("contUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val srcUrl = row("srcUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoShareCode = row("videoShareCode").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoAddr = row("videoAddr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sImgUrl = row("sImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(status) = row("status").getOrElse("")
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(mTime) = row("mTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(gTime) = row("gTime").getOrElse("0000-00-00 00:00:00")
      val resUrl = row("resUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(liveStartTime) = row("liveStartTime").getOrElse("0000-00-00 00:00:00")
      val IntValue(liveStatus) = row("liveStatus").getOrElse("")
      val IntValue(permitUpdate) = row("permitUpdate").getOrElse("")
      val IntValue(screenPopUpStatus) = row("screenPopUpStatus").getOrElse("")
      val updateType = row("updateType").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(videoType) = row("videoType").getOrElse("")
      val displayChannel = row("displayChannel").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoLength = row("videoLength").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawId = row("lotteryDrawId").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val group1 = row("group1").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group2 = row("group2").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group3 = row("group3").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group4 = row("group4").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group5 = row("group5").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group6 = row("group6").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val topicInviter = row("topicInviter").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val IntValue(contentType) = row("contentType").getOrElse(0)
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
       val advImg = row("advImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(listImgType) = row("listImgType").getOrElse(0)
        val IntValue(permitEmotions) = row("permitEmotions").getOrElse(1)
      val IntValue(permitThumbsUp) = row("permitThumbsUp").getOrElse(1)
      SqlFeed(UUID.fromString(feedId), title, serId, srcType, viewStyle, summ, contUrl, srcUrl,
        videoShareCode, videoAddr, sImgUrl, status, new DateTime(cTime), new DateTime(mTime), new DateTime(gTime),
        resUrl, new DateTime(liveStartTime), liveStatus, permitUpdate,
        screenPopUpStatus, updateType, displayChannel, videoLength, lotteryDrawId, videoType, group1, group2, group3,
        group4, group5, group6, contentType, sortNo, topicInviter,listImgType,advImg,permitEmotions,permitThumbsUp)
    }
  }


  def sortFeedsByMTime(feedIds: Seq[String]): Future[Seq[String]] = {
    val ids = feedIds.mkString("','")
    val query = "select feedId from feeds where feedId in('" + ids + "') ORDER BY sortNo,mTime desc"

    client.select[String](query) {
      row =>
        val StringValue(feedId) = row("feedId").getOrElse("")
        feedId
    }
  }

  // 选取用户关注的媒体号下所有资讯
  def collFeedsByUserId(userId: UUID, status: Int, limits: Int, offset: Int): Future[Seq[SqlFeed]] = {
    val query = "select * from feeds where 'jrxy-zb' <> serId and 'ad-h5' <> serId and  serId in (SELECT serId FROM userServices WHERE userId ='" + userId + "') and status=" + status + " ORDER BY sortNo desc, mTime desc limit " + offset + ", " + limits + ""

    client.select[SqlFeed](query) { row =>
      val feedId = row("feedId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val title = row("title").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val serId = row("serId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(srcType) = row("srcType").getOrElse("0")
      val viewStyle = row("viewStyle").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val summ = row("summ").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val contUrl = row("contUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val srcUrl = row("srcUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoShareCode = row("videoShareCode").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoAddr = row("videoAddr").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val sImgUrl = row("sImgUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(status) = row("status").getOrElse("")
      val timeValue(cTime) = row("cTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(mTime) = row("mTime").getOrElse("0000-00-00 00:00:00")
      val timeValue(gTime) = row("gTime").getOrElse("0000-00-00 00:00:00")
      val resUrl = row("resUrl").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val timeValue(liveStartTime) = row("liveStartTime").getOrElse("0000-00-00 00:00:00")
      val IntValue(liveStatus) = row("liveStatus").getOrElse("")
      val IntValue(permitUpdate) = row("permitUpdate").getOrElse("")
      val IntValue(screenPopUpStatus) = row("screenPopUpStatus").getOrElse("")
      val updateType = row("updateType").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(videoType) = row("videoType").getOrElse("")
      val displayChannel = row("displayChannel").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val videoLength = row("videoLength").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val lotteryDrawId = row("lotteryDrawId").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val group1 = row("group1").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group2 = row("group2").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group3 = row("group3").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group4 = row("group4").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group5 = row("group5").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val group6 = row("group6").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val topicInviter = row("topicInviter").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(contentType) = row("contentType").getOrElse(0)
      val IntValue(sortNo) = row("sortNo").getOrElse(0)
       val advImg = row("advImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val IntValue(listImgType) = row("listImgType").getOrElse(0)
      val IntValue(permitEmotions) = row("permitEmotions").getOrElse(1)
      val IntValue(permitThumbsUp) = row("permitThumbsUp").getOrElse(1)
      SqlFeed(UUID.fromString(feedId), title, serId, srcType, viewStyle, summ, contUrl, srcUrl,
        videoShareCode, videoAddr, sImgUrl, status, new DateTime(cTime), new DateTime(mTime), new DateTime(gTime),
        resUrl, new DateTime(liveStartTime), liveStatus, permitUpdate,
        screenPopUpStatus, updateType, displayChannel, videoLength, lotteryDrawId, videoType, group1, group2, group3,
        group4, group5, group6, contentType, sortNo, topicInviter,listImgType,advImg,permitEmotions,permitThumbsUp)
    }
  }
}