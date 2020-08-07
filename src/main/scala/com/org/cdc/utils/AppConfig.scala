package com.org.cdc.utils

import java.util.Properties
import org.apache.commons.io.FilenameUtils

case class AppConfig(props: Properties) {
  val firstFilePath: String = FilenameUtils.getFullPath(props.getProperty("firstFile"))
  val firstFileName: String =FilenameUtils.getName(props.getProperty("firstFile"))

  val secondFilePath: String = FilenameUtils.getFullPath(props.getProperty("secondFile"))
  val secondFileName: String =FilenameUtils.getName(props.getProperty("secondFile"))

  val firstFileBusKeyIdx: String =props.getProperty("firstFileBusinessKeyIndex")
  val secondFileBusKeyIdx: String =props.getProperty("secondFileBusinessKeyIndex")
  val busKeyIdx: String=firstFileBusKeyIdx+"#"+secondFileBusKeyIdx

  val firstFileCdcAttributeIdx: String =props.getProperty("firstFileCdcAttributeIndex")
  val secondFileCdcAttributeIdx: String =props.getProperty("secondFileCdcAttributeIndex")
  val cdcAttributeIdx: String=firstFileCdcAttributeIdx+"#"+secondFileCdcAttributeIdx

  val resultFilePath: String = FilenameUtils.getFullPath(props.getProperty("resultFile"))
  val resultFileName: String =FilenameUtils.getName(props.getProperty("resultFile"))
}
