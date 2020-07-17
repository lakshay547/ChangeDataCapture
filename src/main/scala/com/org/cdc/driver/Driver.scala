package com.org.cdc.driver

import java.io.FileInputStream

import org.apache.spark.sql.SparkSession

import com.org.cdc.dbaccess.HiveAccess
import com.org.cdc.utils.HDFSUtils
import com.org.cdc.functions.ChangeDataCapture

object Driver {
  def main(args: Array[String]): Unit ={

    val hdfsPath=""
    val hdfsFileName=""
    val sourceDBName=""
    val sourceTableName=""
    val targetDBName=""
    val targetTableName=""
    val busKeyIdx=""
    val cdcAttributeIdx=""

    //Creating Spark Session
    val spark=SparkSession.builder
      .enableHiveSupport
      .appName("ChangeDataCapture")
      .getOrCreate()

    //Fetching Data From DataSources into DataFrames
    val firstDF=HDFSUtils.readFromHDFS(s"$hdfsPath",s"$hdfsFileName",spark)
    val secondDF=HiveAccess.readFromHiveTable(s"$sourceDBName",s"$sourceTableName",spark)

    //Calling CDC function
    val finalDF=ChangeDataCapture.performCDC(firstDF,secondDF,s"$busKeyIdx",s"$cdcAttributeIdx")

    //Writing Final DF to target location
    HiveAccess.writeToHiveTable(finalDF,s"$targetDBName",s"$targetTableName")
  }
}
