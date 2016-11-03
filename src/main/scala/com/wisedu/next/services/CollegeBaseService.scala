package com.wisedu.next.services

/**
 * Version: 2.0
 * Author: pattywgm
 * Time: 16/5/19 下午5:19
 * Desc:
 */

import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._

@Singleton
class CollegeBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _

  def getCollegeById(collegeId: String): Future[Option[College]] = {

    appDatabase.colleges.getByIdWithCache(collegeId).flatMap {
      case Some(sysCode) => Future(Some(sysCode))
      case None => appDatabase.colleges.getById(collegeId).map {
        college => college.map { item =>
          appDatabase.colleges.setByIdWithCache(item)
          item
        }
      }
    }
  }


  def collAllColleges(): Future[Seq[College]] = {
    appDatabase.colleges.getAllCollegesWithCache.flatMap {
      colleges => if (colleges.isEmpty) {
        appDatabase.colleges.collAllColleges.map {
          items => {
            appDatabase.colleges.setAllCollegesWithCache(items)
            items
          }
        }
      } else {
        Future(colleges)
      }
    }
  }

  def delCollegeById(collegeId: String): Future[Result] = {
    appDatabase.colleges.delAllCollegesWithCache
    appDatabase.colleges.delById(collegeId)
    appDatabase.colleges.delById(collegeId)
  }

  def insCollege(college: College): Future[Result] = {
    appDatabase.colleges.setByIdWithCache(college)
    appDatabase.colleges.insCollege(college)
  }

  def updCollege(college: College): Future[Result] = {
    appDatabase.colleges.setByIdWithCache(college)
    appDatabase.colleges.updCollege(college)
  }

  def collCollegeSize(region: String): Future[Int] = {
    appDatabase.colleges.collCollegeSize(region)
  }

  def collCollegePageList(limits: Int, offset: Int, region: String): Future[(Seq[College], Int)] = {
    val collegesF = appDatabase.colleges.collColleges(limits, offset, region)
    val collegeSizeF = appDatabase.colleges.collCollegeSize(region)
    for {
      colleges <- collegesF
      collegeSize <- collegeSizeF
    } yield (colleges, collegeSize)
  }

  def collColleges(limits: Int, offset: Int, region: String): Future[Seq[College]] = {
    appDatabase.colleges.collColleges(limits, offset, region)
  }

  //添加院系
  def insDepart(depart: Depart): Future[Result] = {
    appDatabase.departs.delDepartsByCollegeIdWithCache(depart.collegeId)
    appDatabase.departs.insDepart(depart)
  }

  //更新院系
  def updDepart(depart: Depart): Future[Result] = {
    appDatabase.departs.delDepartsByCollegeIdWithCache(depart.collegeId)
    appDatabase.departs.updDepart(depart)
  }
  //刪除院系
  def delDepart(departId: String): Future[Result] = {
    appDatabase.departs.getDepartById(departId).map{
      item => item.map(depart=>appDatabase.departs.delDepartsByCollegeIdWithCache(depart.collegeId))
    }
    appDatabase.departs.delDepart(departId)
  }
  //获取一个院系
  def getDepartById(departId: String): Future[Option[Depart]] = {
    appDatabase.departs.getDepartById(departId)
  }

  //获取学校的院系
  def getDepartsByCollegeId(collegeId: String): Future[Seq[Depart]] = {
    appDatabase.departs.getDepartsByCollegeIdWithCache(collegeId).flatMap {
      departs => if (departs.isEmpty) {
        appDatabase.departs.getDepartsByCollegeId(collegeId).map {
          items => {
            appDatabase.departs.setDepartsByCollegeIdWithCache(collegeId, items)
            items
          }
        }
      } else {
        Future(departs)
      }
    }
  }

  //根据移动校园的编号获取院系
  def getDepartsByCollegeIdAndMId(collegeId: String,mobileId:String): Future[Option[Depart]] = {
    appDatabase.departs.getDepartsByCollegeIdAndMId(collegeId,mobileId)
  }

  def getDepartsByName(departName: String): Future[Option[Depart]] = {
    appDatabase.departs.getDepartsByName(departName)
  }
}
