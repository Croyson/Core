package com.wisedu.next.services

import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql.Result
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._

@Singleton
class SysCodeBaseService {

  @Inject
  @NewSqlDatabase var appDatabase: AppDatabase = _


  def getSysCodeById(typeId: String, value: String): Future[Option[SysCode]] = {
    appDatabase.sysCodes.getByIdWithCache(typeId, value).flatMap {
      case Some(sysCode) => Future(Some(sysCode))
      case None => appDatabase.sysCodes.getById(typeId, value).map {
        sysCode => sysCode.map { item =>
          appDatabase.sysCodes.setByIdWithCache(item)
          item
        }
      }
    }
  }

  def addSysCode(sysCode: SysCode): Future[Result] = {
    appDatabase.sysCodes.setByIdWithCache(sysCode)
    appDatabase.sysCodes.delByTypeIdWithCache(sysCode.typeId)
    appDatabase.sysCodes.insSysCode(sysCode)
  }

  def updateSysCode(sysCode: SysCode): Future[Result] = {
    appDatabase.sysCodes.setByIdWithCache(sysCode)
    appDatabase.sysCodes.delByTypeIdWithCache(sysCode.typeId)
    appDatabase.sysCodes.updateSysCode(sysCode)
  }

  def delSysCode(typeId: String, value: String): Future[Result] = {
    appDatabase.sysCodes.delByTypeIdWithCache(typeId)
    appDatabase.sysCodes.delByIdWithCache(typeId, value)
    appDatabase.sysCodes.delSysCode(typeId, value)
  }

  def getByTypeId(typeId: String): Future[Seq[SysCode]] = {
    appDatabase.sysCodes.getByTypeIdWithCache(typeId).flatMap {
      sysCodes => if (sysCodes.isEmpty) {
        appDatabase.sysCodes.getByTypeId(typeId).map {
          items => {
            appDatabase.sysCodes.setByTypeIdWithCache(typeId, items)
            items
          }
        }
      } else {
        Future(sysCodes)
      }
    }

  }

  def collSysCodes(typeId: String, limit: Int, offSet: Int): Future[Seq[SysCode]] = {
    appDatabase.sysCodes.collSysCodes(typeId, limit, offSet)
  }

  def collSysCodesSize(typeId: String): Future[Int] = {
    appDatabase.sysCodes.collSysCodesSize(typeId)
  }

  def collSysCodePageList(typeId: String, limit: Int, offSet: Int): Future[(Seq[SysCode], Int)] = {
    val sysCodesF = appDatabase.sysCodes.collSysCodes(typeId, limit, offSet)
    val sysCodesSizeF = appDatabase.sysCodes.collSysCodesSize(typeId)
    for {
      sysCodes <- sysCodesF
      sysCodesSize <- sysCodesSizeF
    } yield (sysCodes, sysCodesSize)
  }

}