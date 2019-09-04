package com.barclays.treasury.implementations

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

case class ParquetFileProcessor(inputFilePath: String) {

  def processHdfsFile(): Unit ={

    val sparkSession = new SparkSession.Builder().config("spark.master", "local").getOrCreate()
    val sqlContext = sparkSession.sqlContext

    val schemaPath = ConfigFactory.load().getString("myConfig.schemaPath")
    val newSchemaStructure = AvscSchemaReader(schemaPath).readSchema()

    val inputDataFrame = ParquetFileReader(inputFilePath).readHdfsFile(sqlContext)

    val outputDataFrame = DataFrameModifier().modifySchema(inputDataFrame, newSchemaStructure, sqlContext)

    val hdfsOutputFileRawDataPath = ConfigFactory.load().getString("myConfig.hdfsOutputFileRawDataPath")

    inputDataFrame.show()
    outputDataFrame.show()

    val persistedFile = ParquetFilePersister(hdfsOutputFileRawDataPath).persistHdfsFile(outputDataFrame)
    val renamedFile = FileProcessor().renameFile(persistedFile, new File(inputFilePath).getName)

    val destinationPath = FileProcessor().getOutputFilePath(inputFilePath)

    FileProcessor().copyFile(renamedFile, destinationPath)

  }
}
