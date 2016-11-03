package com.wisedu.next.services

import javax.inject.{Inject, Singleton}

import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import com.wisedu.next.annotations.NewSqlDatabase
import com.wisedu.next.models._
import org.joda.time.DateTime

@Singleton
class ServiceBaseService {

  @Inject @NewSqlDatabase var appDatabase: AppDatabase = _

  def getServiceById(serId: String): Future[Option[Service]] ={
    appDatabase.services.getById(serId)
  }

  def delServiceById(serId: String): Future[Result] = {
    appDatabase.services.delById(serId)
  }

  def insService(service: Service): Future[Result] = {
    appDatabase.services.insService(service)
  }

  def updService(service: Service): Future[Result] = {
    appDatabase.services.updService(service)
  }

  def collServices(name: String, srcType: String, orgType: String, limit: Int, offSet: Int): Future[Seq[Service]] = {
    appDatabase.services.collServices(name,srcType,orgType,limit,offSet)
  }

  def collServicesSize(name: String, srcType: String, orgType: String): Future[Int] = {
     appDatabase.services.collServicesSize(name,srcType,orgType)
  }

  def collServicePageList(name: String, srcType: String, orgType: String, limit: Int, offSet: Int): Future[(Seq[Service], Int)] = {
    val servicesF = appDatabase.services.collServices(name,srcType,orgType,limit,offSet)
    val servicesSizeF =  appDatabase.services.collServicesSize(name,srcType,orgType)
    for {
      services <- servicesF
      servicesSize <- servicesSizeF
    } yield (services, servicesSize)
  }

  def getServiceCrawlerById(serId: String): Future[Option[ServiceCrawler]] = {
    appDatabase.serviceCrawlers.getById(serId)
  }

  def collServiceCrawlers():Future[Seq[ServiceCrawler]] ={
    appDatabase.serviceCrawlers.collServiceCrawlers()
  }

  def collServices(limits: Int, offset: Int): Future[Seq[Service]] = {
    appDatabase.services.collServices(limits, offset)
  }

  def collServicesByCollegeId(college_id: String, filterType: String, limit: Int, offSet: Int): Future[Seq[Service]] = {
    appDatabase.services.collServicesByCollegeId(college_id, filterType, limit, offSet)
  }

  def updServiceMTime(serId:String,mTime:DateTime): Future[Result] = {
    appDatabase.services.updServiceMTime(serId,mTime)
  }
}