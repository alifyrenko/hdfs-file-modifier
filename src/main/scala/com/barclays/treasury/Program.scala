package com.barclays.treasury

import com.barclays.treasury.implementations.{FileListRetriever, ParquetFileProcessor}

object Program extends App{

  val parquetFilesPaths =  FileListRetriever().retrieveFileList()
  parquetFilesPaths.toArray.foreach(f => ParquetFileProcessor(f.toString).processHdfsFile())

}
