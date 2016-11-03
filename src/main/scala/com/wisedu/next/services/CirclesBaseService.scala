package com.wisedu.next.services

import java.util.UUID
import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.consts.CircleKeys
import com.wisedu.next.models._

@Singleton
class CirclesBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  //获取一个圈子
  def getCircleById(circleId: String): Future[Option[Circle]] = {
    appDatabase.circles.getByIdWithCache(circleId).flatMap {
      case Some(circle) => Future(Some(circle))
      case None => appDatabase.circles.getById(circleId).map {
        circle => circle.map { item =>
          appDatabase.circles.setByIdWithCache(circleId, item)
          item
        }
      }
    }


  }

  //删除一个圈子
  def delCircleById(circleId: String): Future[Result] = {
    appDatabase.circles.delByIdWithCache(circleId)
    appDatabase.circles.delById(circleId)
  }

  //查询圈子列表
  def collCircles(collegeId: String, circleName: String, circleType: String, strutsType: String, limit: Int, offSet: Int): Future[Seq[Circle]] = {
    val key = CircleKeys.circleList.format(collegeId, circleName, circleType, strutsType, limit, offSet)
    appDatabase.circles.getCirclesByKeyWithCache(key).flatMap {
      circles => if (circles.isEmpty) {
        appDatabase.circles.collCircles(collegeId, circleName, circleType, strutsType, limit, offSet).map {
          items => appDatabase.circles.setCirclesByKeyCache(key, items)
            items
        }
      } else {
        Future(circles)
      }
    }

  }

  def collCirclesSize(collegeId: String, circleName: String, circleType: String, strutsType: String): Future[Int] = {
    appDatabase.circles.collCircleSize(collegeId, circleName, circleType, strutsType)
  }


  def collCirclesPageList(collegeId: String, circleName: String, circleType: String, strutsType: String, limit: Int, offSet: Int): Future[(Seq[Circle], Int)] = {
    val circlesF = appDatabase.circles.collCircles(collegeId, circleName, circleType, strutsType, limit, offSet)
    val circlesSizeF = appDatabase.circles.collCircleSize(collegeId, circleName, circleType, strutsType)
    for {
      circles <- circlesF
      circlesSize <- circlesSizeF
    } yield (circles, circlesSize)
  }


  //新增圈子
  def addCircle(circle: Circle): Future[Result] = {
    appDatabase.circles.insCircle(circle)
  }

  //修改圈子
  def updCircle(circle: Circle): Future[Result] = {
    appDatabase.circles.delByIdWithCache(circle.circleId)
    appDatabase.circles.updCircle(circle)
  }

  //根据圈子编号获取圈子的限制条件
  def getPermissionsByCircleId(circleId: String): Future[Seq[CirclePermission]] = {
    appDatabase.circlePermissions.getByCircleId(circleId)
  }

  //根据学校编号获取圈子的限制条件
  def getPermissionsByCollegeId(collegeId: String): Future[Seq[CirclePermission]] = {
    appDatabase.circlePermissions.getByCollegeId(collegeId, "", "")
  }

  //根据圈子编号删除限制条件
  def delPermissionsByCircleId(circleId: String): Future[Result] = {
    appDatabase.circlePermissions.getByCircleId(circleId).map{
      items => Future.collect(items.map{
        item =>  appDatabase.circles.delRecommendByIdWithCache(item.collegeId)
      })
    }.flatMap{
      rst =>  appDatabase.circlePermissions.delById(circleId)
    }
  }

  //增加圈子的限制条件
  def addCirclePermission(circlePermission: CirclePermission): Future[Result] = {
    appDatabase.circlePermissions.insCirclePermission(circlePermission)
  }

  //获取学校的默认展示圈
  def getCollegeDefaultDisPlayCircle(collegeId: String): Future[Option[Circle]] = {
    appDatabase.circlePermissions.getByCollegeId(collegeId, "1", "").map(_.headOption).flatMap {
      case Some(item) => getCircleById(item.circleId)
      case None => Future(None)
    }
  }

  //获取学校的默认写入圈
  def getCollegeDefaultSaveCircle(collegeId: String): Future[Option[Circle]] = {
    appDatabase.circlePermissions.getByCollegeId(collegeId, "", "1").map(_.headOption).flatMap {
      case Some(item) => getCircleById(item.circleId)
      case None => Future(None)
    }
  }

  /*
   * 根据消息编码获取圈子信息
   *
   * @param messageId   消息编号
   *
   * */
  def getCircleByMsgId(messageId: String): Future[Option[Circle]] = {
    appDatabase.messageInfos.getMsgById(UUID.fromString(messageId)).flatMap {
      case Some(msg) => if (msg.groupId.nonEmpty) {
        getCircleById(msg.groupId)
      } else {
        Future(None)
      }
      case None => Future(None)
    }
  }

  def getRecommendCircle(collegeId:String): Future[Option[Circle]] = {
    appDatabase.circles.getRecommendByIdWithCache(collegeId).flatMap {
      case Some(circle) => Future(Some(circle))
      case None => appDatabase.circles.getRecommendCircle(collegeId).map {
        circle => circle.map { item =>
          appDatabase.circles.setRecommendByIdWithCache(collegeId, item)
          item
        }
      }
    }
  }
}