package com.org.cdc.dbaccess

import org.apache.spark.sql.{DataFrame, SparkSession}


object HiveAccess {
  def readFromHiveTable(dbName:String,tableName: String,spark: SparkSession,columns: String="*"): DataFrame= {
    val df=spark.sql(s"select $columns from $dbName.$tableName")
    return df
  }

  def writeToHiveTable(df: DataFrame,dbName: String,tableName: String): Unit ={
    df.write.mode("append").insertInto(s"$dbName.$tableName")
  }
}
