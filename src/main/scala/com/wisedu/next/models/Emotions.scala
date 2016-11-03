package com.wisedu.next.models

import com.twitter.finagle.exp.mysql.StringValue
import com.twitter.util.Future
import com.wisedu.next.types.NextTypes.MysqlClient

case class Emotion(modelId: String, selectId: String, selectName: String, selectImg: String, mark: String)

abstract class ConcreteEmotions(val client: MysqlClient) {

  def getByModeId(modeId: String): Future[Seq[Emotion]] = {
    val sql = "select * from emotions where modelId = ?"
    client.prepare(sql).select[Emotion](modeId) { row =>
     val modelId = row("modelId").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val  selectId = row("selectId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
       val  selectName = row("selectName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
     val  selectImg = row("selectImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  mark = row("mark").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      Emotion(modelId, selectId, selectName, selectImg,mark)
      }
  }

  def getByModeIdAndSelectId(modelId: String, selectId: String): Future[Option[Emotion]] = {
    val sql = "select * from emotions where modelId = ? and selectId = ?"
    client.prepare(sql).select[Emotion](modelId, selectId) { row =>
      val modelId = row("modelId").map {
        case StringValue(str) => str
        case _ => ""
      }.get

      val  selectId = row("selectId").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  selectName = row("selectName").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  selectImg = row("selectImg").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      val  mark = row("mark").map {
        case StringValue(str) => str
        case _ => ""
      }.get
      Emotion(modelId, selectId, selectName, selectImg,mark)
    }.map(_.headOption)
  }

}