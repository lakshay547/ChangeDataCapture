package com.org.cdc

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSessionTrait extends BeforeAndAfterAll{ self : Suite =>
  var spark : SparkSession = _
  override def beforeAll(): Unit={
    spark=SparkSession.builder().appName("CDC").getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.close()
  }

}
