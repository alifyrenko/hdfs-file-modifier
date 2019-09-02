package com.barclays.treasury.implementations

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

case class ParquetFileProcessor(path: String) {

  def processHdfsFile(): Unit ={

    val sparkSession = new SparkSession.Builder().config("spark.master", "local").getOrCreate()
    val sqlContext = sparkSession.sqlContext

    val schemaPath = ConfigFactory.load().getString("myConfig.schemaPath")
    val newSchemaStructure = AvscSchemaReader(schemaPath).readSchema()

    val inputDataFrame = ParquetFileReader(path).readHdfsFile(sqlContext)

    val outputDataFrame = DataFrameModifier().modifySchema(inputDataFrame, newSchemaStructure, sqlContext)

    val hdfsFileToWritePath = ConfigFactory.load().getString("myConfig.hdfsFileToWritePath")

    inputDataFrame.show()
    outputDataFrame.show()

    ParquetFilePersister(hdfsFileToWritePath).persistHdfsFile(outputDataFrame)
  }
}
