package com.org.cdc.utils

object ScalaUtils {

  def mapIdxAndCol(idx: String,colNames: Array[String]): List[String] ={
    val ranges = idx.split("&")
    var idxArr: Array[Int] = Array.empty[Int]
    for (range <- ranges) {
      val rangeList: List[Int] = range.split("-").toList.map(f => f.toInt)
      val rangeSeq = List.range(rangeList.head.toInt, rangeList.last.toInt + 1)
      idxArr = idxArr ++ rangeSeq
    }
    val mappingColList = (idxArr map colNames).toList
    mappingColList
  }

  def mapMultipleIdxWithColList(IdxString: String, firstcolNamesList: Array[String],secondcolNamesList: Array[String]): (List[String],List[String]) ={
    val idxArr=IdxString.split("#")
    val firstList=mapIdxAndCol(idxArr(0),firstcolNamesList)
    val secondList=mapIdxAndCol(idxArr(1),secondcolNamesList)

    (firstList,secondList)
  }
}
