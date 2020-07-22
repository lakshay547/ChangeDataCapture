package com.org.cdc.dbaccess

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.org.cdc.utils.ScalaUtils

object HiveAccess {
  def readFromHiveTable(dbName:String,tableName: String,spark: SparkSession,columns: String="*"): DataFrame= {
    val df=spark.sql(s"select $columns from $dbName.$tableName")
    df
  }

  def readFromHiveTableWithColIdx(dbName: String,tableName: String,spark: SparkSession,colIdx: String): DataFrame ={
    val hiveSchema=spark.table(s"$dbName.$tableName").schema
    val colNamesList=hiveSchema.names
    val requiredColumns=ScalaUtils.mapIdxAndCol(colIdx,colNamesList).mkString(",")
    readFromHiveTable(dbName,tableName,spark,requiredColumns)
  }

  def writeToHiveTable(df: DataFrame,dbName: String,tableName: String): Unit ={
    df.write.mode("append").insertInto(s"$dbName.$tableName")
  }

}
