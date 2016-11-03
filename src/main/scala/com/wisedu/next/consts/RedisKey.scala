package com.wisedu.next.consts


object SysCodeKeys{
  /*根据typeId和value获取syscode*/
  val SysCodeTypeIdAndValue         = "sysCode_%s_%s"
  /*根据typeId获取syscode集合*/
  val SysCodeTypeId         = "sysCode_%s"

}
object CollegeKeys{
  /*获取学校编号对应的学校信息*/
  val  CollegeId         = "college_%s"
  /*获取所有的学校集合*/
  val Colleges         = "colleges"

  /*获取一个院系*/
  val DepartsCollegeId ="departsCollegeId _%s"

}

object UserKeys{
  /*获取用忽的key*/
  val  userId         = "user_%s"

  val  userMsgCount  = "user_%s_msgCount"

  val  userMsgListLastTime = "user_%s_msgListLastTime"

}

object CircleKeys{
  /*获取用忽的key*/
  val  circleId        = "circle_%s"

  val circleMsgId     = "circle_msg_%s"

  val circleRecommendId     = "circle_recommend_%s"

  val circleList    = "circle_%s_%s_%s_%s_%s_%s"
}


