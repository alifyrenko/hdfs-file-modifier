package com.barclays.treasury.implementations

import java.io.File
import java.util

import com.barclays.treasury.traits.IFileListRetriever
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils

case class FileListRetriever() extends IFileListRetriever{

  def retrieveFileList(): util.Collection[File] ={

    val config = ConfigFactory.load()
    val hdfsFileToReadPath = config.getString("myConfig.hdfsFileToReadPath")
    val fileExtension = config.getString("myConfig.fileExtension")

    return  FileUtils.listFiles(new File(hdfsFileToReadPath),Array(fileExtension), true)
  }
}
