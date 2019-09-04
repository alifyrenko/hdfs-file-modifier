package com.barclays.treasury.implementations

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils

import scala.collection.mutable.ListBuffer

case class FileProcessor() {

  def copyFile(sourceFile: File, destinationPath: String) = {
    FileUtils.copyFileToDirectory(sourceFile, new File(destinationPath))
  }

  def renameFile(sourceFile: File, finalName: String): File = {
    val newFile = new File(s"${sourceFile.getParent}\\$finalName")
    sourceFile.renameTo(newFile)

    return newFile
  }

  def getOutputFilePath (inputFilePath: String): String = {

    val dataSetName = ConfigFactory.load().getString("myConfig.dataSetName")
    val tailFileFolderPath = inputFilePath.split(dataSetName)(1)

    val rootOutputPath = ConfigFactory.load().getString("myConfig.hdfsOutputFileDataPath")

    val fileName = new File(inputFilePath).getName

    val tailFolderPath = tailFileFolderPath.split(fileName)(0)

    return  s"$rootOutputPath$dataSetName$tailFolderPath"
  }

  def retrieveFilePathList(path: String): util.Collection[File] ={

    val config = ConfigFactory.load()
    val fileExtension = config.getString("myConfig.fileExtension")

    return  FileUtils.listFiles(new File(path),Array(fileExtension), true)
  }

  def retrieveFileList(path: String): List[File] = {

    val filePathsList = retrieveFilePathList(path)
    val fileList = ListBuffer[File]();
    filePathsList.toArray.foreach(f => fileList += new File(f.toString))

    return fileList.toList
  }

  def getLastGeneratedFile(path: String): File ={

    val fileList = retrieveFileList(path)
    return fileList.maxBy(f => f.lastModified())
  }
}
