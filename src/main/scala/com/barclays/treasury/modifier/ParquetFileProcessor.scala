package com.barclays.treasury.modifier

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

case class ParquetFileProcessor() {

  def modifyParquetFiles(): Unit ={
    val parquetFilesPaths =  FileManager().retrieveFilePathList(ConfigFactory.load().getString("myConfig.hdfsInputFilePath"))
    parquetFilesPaths.foreach(f => ParquetFileProcessor().modifyParquetFile(f.toString))
  }

  private def modifyParquetFile(inputFilePath: String): Unit ={

    val sparkSession = new SparkSession.Builder().config("spark.master", "local").getOrCreate()
    val sqlContext = sparkSession.sqlContext

    val schemaPath = ConfigFactory.load().getString("myConfig.schemaPath")
    val newSchemaStructure = AvscSchemaReader(schemaPath).readSchema()

    val inputDataFrame = sqlContext.read.parquet(inputFilePath)

    val outputDataFrame = DataFrameModifier().modifySchema(inputDataFrame, newSchemaStructure, sqlContext)

    val hdfsOutputFileRawDataPath = ConfigFactory.load().getString("myConfig.hdfsOutputFileRawDataPath")

    outputDataFrame.write.mode("Overwrite").parquet(hdfsOutputFileRawDataPath)
    val persistedFile = FileManager().retrieveLastGeneratedFile(hdfsOutputFileRawDataPath)

    val renamedFile = FileManager().renameFile(persistedFile, new File(inputFilePath).getName)

    val destinationPath = FileManager().retrieveOutputFilePath(inputFilePath)

    FileUtils.copyFileToDirectory(renamedFile, new File(destinationPath))
  }
}
