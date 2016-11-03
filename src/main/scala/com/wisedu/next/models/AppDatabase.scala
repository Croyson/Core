package com.wisedu.next.models

import com.twitter.finagle.redis.Client
import com.websudos.phantom.connectors.KeySpaceDef
import com.websudos.phantom.dsl._
import com.wisedu.next.types.NextTypes.MysqlClient

class AppDatabase(val keyspace: KeySpaceDef, val mysqlClient: MysqlClient, val redisClient: Client) extends Database(keyspace) {

  object feeds extends ConcreteFeeds(mysqlClient) with keyspace.Connector

  object colleges extends ConcreteColleges(mysqlClient, redisClient)

  object feedbacks extends ConcreteFeedbacks with keyspace.Connector

  object feedCollects extends ConcreteFeedCollects(mysqlClient)

  object feedLikes extends ConcreteFeedLikes(mysqlClient)

  object feedPermissions extends ConcreteFeedPermissions(mysqlClient)

  object feedReadDetails extends ConcreteFeedReadDetails with keyspace.Connector

  object feedReadStats extends ConcreteFeedReadStats(mysqlClient)

  object feedStats extends ConcreteFeedStats(mysqlClient)

  object feedTags extends ConcreteFeedTags(mysqlClient)

  object feedUpdates extends ConcreteFeedUpdates(mysqlClient)

  object feedLotteryDraw extends ConcreteFeedLotteryDraws(mysqlClient)

  object feedShares extends ConcreteFeedShares with keyspace.Connector

  object feedAndStats extends ConcreteFeedAndStats(mysqlClient)

  object groupPermissions extends ConcreteGroupPermissions(mysqlClient)

  object groups extends ConcreteGroups(mysqlClient)

  object groupFeeds extends ConcreteGroupFeeds(mysqlClient)

  object sysCodes extends ConcreteSysCodes(mysqlClient, redisClient)

  object tags extends ConcreteTags(mysqlClient)

  object tagStructs extends ConcreteTagStructs(mysqlClient)

  object updateLikes extends ConcreteUpdateLikes(mysqlClient)

  object services extends ConcreteServices(mysqlClient)

  object users extends ConcreteUsers(mysqlClient, redisClient)

  object userServices extends ConcreteUserServices(mysqlClient)

  object userSnsInfos extends ConcreteUserSnsInfos(mysqlClient)

  object userTags extends ConcreteUserTags(mysqlClient)

  object userTagKeys extends ConcreteUserTagKeys(mysqlClient)

  object serviceCrawlers extends ConcreteServiceCrawlers with keyspace.Connector

  object authCodes extends ConcreteAuthCodes with keyspace.Connector

  object adminTokens extends ConcreteAdminTokens with keyspace.Connector

  object adminUsers extends ConcreteAdminUsers with keyspace.Connector

  object pushs extends ConcretePushs(mysqlClient)

  object departs extends ConcreteDeparts(mysqlClient, redisClient)

  object emotions extends ConcreteEmotions(mysqlClient)

  object emotionCommunicate extends ConcreteEmotionCommunicates(mysqlClient)

  object messageInfos extends ConcreteMessageInfos(mysqlClient, redisClient) with keyspace.Connector

  object circlePermissions extends ConcreteCirclePermissions(mysqlClient, redisClient)

  object circles extends ConcreteCircles(mysqlClient, redisClient)


  object userStats extends ConcreteUserStats(mysqlClient,redisClient) with keyspace.Connector

  object userRelations extends ConcreteUserRelations(mysqlClient, redisClient)

}
