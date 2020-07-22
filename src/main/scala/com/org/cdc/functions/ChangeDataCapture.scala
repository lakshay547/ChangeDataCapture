package com.org.cdc.functions

import org.apache.spark.sql.{DataFrame,Row}
import org.apache.spark.sql.functions.{udf,md5,struct,col,lit}
import com.org.cdc.utils.ScalaUtils

object ChangeDataCapture {

    val concat_row = udf((r: Row) => {
    val s = r.mkString("|")
    s
    })

  def createHashColumn(df: DataFrame,colList: List[String],hashColName: String):DataFrame={
    return df.withColumn(s"$hashColName",md5(concat_row(struct(colList.head,colList.tail:_*))))
  }

  def performCDC(srcDF: DataFrame,tgtDF: DataFrame,busKeyIdx: String,cdcAttributesIdx: String): DataFrame ={

    //Renaming Target DF columns to avoid ambiguity
    var renamedTgtDF=tgtDF
    tgtDF.columns.foreach(i=> renamedTgtDF= tgtDF.withColumnRenamed(s"$i",s"${i}_target"))

    //Mapping Business Keys and CDC Attributes to column Names
    val (srcBusKeys,tgtBusKeys)=ScalaUtils.mapMultipleIdxWithColList(busKeyIdx,srcDF.columns,renamedTgtDF.columns)
    val (srcCdcAttr,tgtCdcAttr)=ScalaUtils.mapMultipleIdxWithColList(cdcAttributesIdx,srcDF.columns,renamedTgtDF.columns)

    //Creating Hash Column for Business Key and CDC Attributes
    val hashSrcDF=createHashColumn(createHashColumn(srcDF,srcBusKeys,"busKeyHash"),srcCdcAttr,"srcAttrHash")
    val hashTgtDF=createHashColumn(createHashColumn(renamedTgtDF,tgtBusKeys,"busKeyHash"),tgtCdcAttr,"tgtAttrHash")

    //Left Joining Both DataFrames
    val joinedDF=hashSrcDF.join(hashTgtDF,Seq("busKeyHash"),"left")

    //Filtering out New Records and assigning Flag "I"
    val insertDF=joinedDF.filter(col(srcBusKeys(0)).isNull)
      .withColumn("CDC_Flag",lit("I"))
    val otherRecords=joinedDF.filter(col(srcBusKeys(0)).isNotNull)

    //Filtering out Updated Records and assigning Flag "U"
    val updatedDF=otherRecords.filter(col("srcAttrHash")=!=col("tgtAttrHash"))
      .withColumn("CDC_Flag",lit("U"))
    val oldRecordsDF=otherRecords.filter(col("srcAttrHash")===col("tgtAttrHash"))
      .withColumn("CDC_Flag",lit("O"))

    //Consolidating All the Records
    val consolidatedDF=insertDF.union(updatedDF.union(oldRecordsDF))

    consolidatedDF
  }
}
