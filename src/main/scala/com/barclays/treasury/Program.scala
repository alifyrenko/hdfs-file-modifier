package com.barclays.treasury

import com.barclays.treasury.implementations.{FileProcessor, ParquetFileProcessor}
import com.typesafe.config.ConfigFactory

object Program extends App{

  val parquetFilesPaths =  FileProcessor().retrieveFilePathList(ConfigFactory.load().getString("myConfig.hdfsInputFilePath"))
  parquetFilesPaths.toArray.foreach(f => ParquetFileProcessor(f.toString).processHdfsFile())
}
