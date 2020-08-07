package com.org.cdc.dao

import org.apache.spark.sql.{DataFrame, SparkSession}

object LocalFileAccess {

  def readFile(filePath: String,fileName: String,spark: SparkSession): DataFrame={
    spark.read.option("header","true").csv(s"$filePath/$fileName")
  }

  def writeFile(df: DataFrame,filePath: String,fileName: String): Unit ={
    df.coalesce(1).write.mode("overwrite").option("header","true").csv(s"$filePath/$fileName")
  }
}
