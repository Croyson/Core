package com.wisedu.next.models

import com.twitter.util.Future
import com.websudos.phantom.dsl._
import com.websudos.phantom.CassandraTable

case class ServiceCrawler(serId: String, crawlers: Map[String, String])

class ServiceCrawlers extends CassandraTable[ServiceCrawlers, ServiceCrawler] {
  object serId extends StringColumn(this) with PrimaryKey[String]
  object crawlers extends MapColumn[ServiceCrawlers, ServiceCrawler, String, String](this)

  def fromRow(row: Row): ServiceCrawler = {
    ServiceCrawler(
      serId(row), crawlers(row)
    )
  }
}

abstract class ConcreteServiceCrawlers extends ServiceCrawlers with RootConnector {

  def getById(serId: String): Future[Option[ServiceCrawler]] = {
    select.where(_.serId eqs serId).get()
  }

  def collServiceCrawlers():Future[Seq[ServiceCrawler]] ={
    select.collect()
  }


}