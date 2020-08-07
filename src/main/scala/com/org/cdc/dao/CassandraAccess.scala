package com.org.cdc.dao

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.datastax.spark.connector._

object CassandraAccess {
  def readFromCassandraTable(keySpace:String,tableName: String,spark: SparkSession):DataFrame={
      val df= spark.read.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> s"$tableName","keySpace" -> s"$keySpace"))
        .load()
      df
  }
  def writeToCassandraTable(df:DataFrame,keySpace:String,tableName: String): Unit ={
    df.rdd.saveToCassandra(keySpace,tableName)
  }
}
