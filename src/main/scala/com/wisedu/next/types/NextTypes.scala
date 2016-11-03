package com.wisedu.next.types

import com.twitter.finagle.exp.mysql.{Transactions, Client}

/**
 * NextTypes
 *
 * @author croyson
 *         contact 14116004@wisedu.com
 *         date 16/5/13
 */
object NextTypes {
  type MysqlClient = Client with Transactions
}
