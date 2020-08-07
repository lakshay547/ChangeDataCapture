package com.org.cdc.dbaccess

import com.org.cdc.SparkSessionTrait
import com.org.cdc.dao.HiveAccess
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.Assertions
import org.scalatest.funsuite.AnyFunSuite
import org.mockito.Mockito.{times, verify, when}
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.mockito.MockitoSugar

@RunWith(classOf[JUnitRunner])
class HiveAccessTest extends AnyFunSuite with MockitoSugar with SparkSessionTrait {

  val mockedSparkSession: SparkSession =mock[SparkSession]
  val mockedDataFrameReader: DataFrameReader =mock[DataFrameReader]
  val mockedInputDF: DataFrame =mock[DataFrame]
  when(mockedSparkSession.read).thenReturn(mockedDataFrameReader)
  import mockedSparkSession.implicits._

  test("readFromHiveTable"){
    val testDF=Seq(("val1","val2")).toDF("col1","col2")
    when(mockedSparkSession.sql("")).thenReturn(testDF)

    val df=HiveAccess.readFromHiveTable("dbName","tableName",mockedSparkSession)
    assert(testDF == df)
  }


  test("writeToHiveTable"){
    val mockDF=mock[DataFrame]
    val mockedDataFrameWriter: DataFrameWriter[Row] =mock[DataFrameWriter[Row]]
    when(mockDF.write).thenReturn(mockedDataFrameWriter)
    when(mockedDataFrameWriter.mode("")).thenReturn(mockedDataFrameWriter)

    HiveAccess.writeToHiveTable(mockDF,"dbName","tableName")
    verify(mockedDataFrameWriter,times(1)).insertInto("")

    //test exception
    when(mockDF.write).thenThrow(new RuntimeException())
    Assertions.intercept[Exception]{
        HiveAccess.writeToHiveTable(mockDF,"dbName","tableName")
    }
  }

}
