package com.wisedu.next.models

import java.util.{TimeZone, UUID}

import com.twitter.finagle.exp.mysql.{IntValue, LongValue, StringValue, TimestampValue}
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes._
import org.joda.time.DateTime

/**
 * Version: 1.1
 * Author: pattywgm
 * Time: 16/6/2 上午9:47
 * Desc: 封装一个Feed和FeedStat的model
 */


case class FeedAndStats(feedId: UUID, title: String, serId: String, srcType: Int, viewStyle: String, summ: String,
                        contUrl: String, srcUrl: String, videoShareCode: String, videoAddr: String,
                        sImgUrl: String, status: Int, cTime: DateTime, mTime: DateTime, gTime: DateTime,
                        resUrl: String, liveStartTime: DateTime, liveStatus: Int, permitUpdate: Int,
                        screenPopUpStatus: Int, updateType: String,
                        displayChannel: String, videoLength: String, lotteryDrawId: String, videoType: Int, group1: String,
                        group2: String, group3: String, group4: String, group5: String, group6: String, contentType: Int,
                        readNum: Long, likeNum: Long, unLikeNum: Long, collectNum: Long, updateNum: Long, shareNum: Long,
                        onlineNum: Long, voteNum: Long, imgNum: Long, sortNo: Int, topicInviter: String,
                        listImgType: Int, advImg: String, permitEmotions: Int, permitThumbsUp: Int)

abstract class ConcreteFeedAndStats(val client: MysqlClient) {
  val timeValue = new TimestampValue(TimeZone.getDefault, TimeZone.getDefault)

  def collFeeds(groupId: String, groupStrutsType: String, tag: String, viewStyles: String, tags: String, state: String,
                orderInfo: String, userId: String, limits: Int, offSet: Int): Future[Seq[FeedAndStats]] = {
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
        query += " and  EXISTS  (select feedId from groupFeeds gf where  groupId = '" + groupId + "' and auditStatus = 1 and f.feedId =  gf.feedId )  "
      } else if ("3".equals(groupStrutsType)) {
        //虚拟组是通过用户关联的校园号查询
        query += " and  EXISTS  (SELECT serId FROM userServices us WHERE userId ='" + userId + "' and 'jrxy-zb' <> serId and 'ad-h5' <> serId and us.serId = f.serId ) "
      } else {
        query += " and 1 = 2 "
      }

    }

    if (!orderInfo.isEmpty) {
      query += " order by " + orderInfo
    } else {
      query += " order by sortNo desc,mTime desc "
    }

    query += "limit " + offSet + "," + limits

    client.select[FeedAndStats](query) { row =>
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
      val readNum = row("readNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val likeNum = row("likeNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val unLikeNum = row("unLikeNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val collectNum = row("collectNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val updateNum = row("updateNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val shareNum = row("shareNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val onlineNum = row("onlineNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val voteNum = row("voteNum").map {
        case LongValue(str) => str
        case _ => 0L
      }.get
      val imgNum = row("imgNum").map {
        case LongValue(str) => str
        case _ => 0L
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
      FeedAndStats(UUID.fromString(feedId), title, serId, srcType, viewStyle, summ, contUrl, srcUrl,
        videoShareCode, videoAddr, sImgUrl, status, new DateTime(cTime), new DateTime(mTime), new DateTime(gTime),
        resUrl, new DateTime(liveStartTime), liveStatus, permitUpdate,
        screenPopUpStatus, updateType, displayChannel, videoLength, lotteryDrawId,videoType,group1,group2,group3,
        group4,group5,group6,contentType, readNum, likeNum, unLikeNum, collectNum, updateNum, shareNum, onlineNum, voteNum,
        imgNum,sortNo,topicInviter,listImgType,advImg,permitEmotions,permitThumbsUp)
    }
  }
}

