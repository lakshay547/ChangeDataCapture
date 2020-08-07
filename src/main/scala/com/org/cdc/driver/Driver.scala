package com.org.cdc.driver

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.commons.io.FilenameUtils
import com.org.cdc.dao.LocalFileAccess
import com.org.cdc.utils.AppConfig
import com.org.cdc.functions.ChangeDataCapture


object Driver {
  def main(args: Array[String]): Unit ={
    if(args.length <1){
      println("Properties File Path not passed !!!")
      System.exit(-1)
    }
    println("Loading properties file !!!")
    val props=new Properties()
    props.load(new FileInputStream(FilenameUtils.getFullPath(args(0)) + FilenameUtils.getName(args(0))))
    val appConfig=AppConfig(props)
    println("Properties File Loaded !!!")

    val firstFilePath=appConfig.firstFilePath
    val firstFileName=appConfig.firstFileName
    val secondFilePath=appConfig.secondFilePath
    val secondFileName=appConfig.secondFileName
    val busKeyIdx=appConfig.busKeyIdx
    val cdcAttributeIdx=appConfig.cdcAttributeIdx
    val resultFilePath=appConfig.resultFilePath
    val resultFileName=appConfig.resultFileName

    //Creating Spark Session
    println("Starting Spark Session !!!")
    val spark=SparkSession.builder
      .appName("ChangeDataCapture")
      .master("local")
      .getOrCreate()
    println("Spark Session Created !!!")


    //Fetching Data From DataSources into DataFrames
    println("Loading Data Files !!!")
    val firstDF=LocalFileAccess.readFile(firstFilePath,firstFileName,spark)
    val secondDF=LocalFileAccess.readFile(secondFilePath,secondFileName,spark)

    println("First Data Frame !!!")
    firstDF.show()
    println("Second Data Frame !!!")
    secondDF.show()

    //Calling CDC function
    println("CDC function called !!!")
    val finalDF=ChangeDataCapture.performCDC(firstDF,secondDF,busKeyIdx,cdcAttributeIdx)

    println("Final Data Frame !!!")
    finalDF.show()

    //Writing Final DF to target location
    println("Writing Result to File !!!")
    LocalFileAccess.writeFile(finalDF,resultFilePath,resultFileName)
    println("Result committed to File !!!")
    spark.close()
  }
}
