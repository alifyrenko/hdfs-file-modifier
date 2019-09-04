package com.barclays.treasury.modifier

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils

import scala.collection.mutable.ListBuffer

case class FileManager() {

  def renameFile(sourceFile: File, finalName: String): File = {
    val newFile = new File(s"${sourceFile.getParent}\\$finalName")
    sourceFile.renameTo(newFile)

    return newFile
  }

  def retrieveOutputFilePath(inputFilePath: String): String = {

    val dataSetName = ConfigFactory.load().getString("myConfig.dataSetName")
    val tailFileFolderPath = inputFilePath.split(dataSetName)(1)
    val rootOutputPath = ConfigFactory.load().getString("myConfig.hdfsOutputFileDataPath")
    val fileName = new File(inputFilePath).getName
    val tailFolderPath = tailFileFolderPath.split(fileName)(0)

    return  s"$rootOutputPath$dataSetName$tailFolderPath"
  }

  def retrieveFilePathList(path: String): Array[AnyRef] ={

    val config = ConfigFactory.load()
    val fileExtension = config.getString("myConfig.fileExtension")

    val fileList = FileUtils.listFiles(new File(path),Array(fileExtension), true)

    return  fileList.toArray
  }

  def retrieveFileList(path: String): List[File] = {

    val filePathsList = retrieveFilePathList(path)
    val fileList = ListBuffer[File]()
    filePathsList.foreach(f => fileList += new File(f.toString))

    return fileList.toList
  }

  def retrieveLastGeneratedFile(path: String): File ={

    val fileList = retrieveFileList(path)
    return fileList.maxBy(f => f.lastModified())
  }
}
