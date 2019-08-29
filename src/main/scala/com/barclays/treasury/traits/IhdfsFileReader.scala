package com.barclays.treasury.traits

import org.apache.spark.sql.{DataFrame, SQLContext}

trait IhdfsFileReader {
  def readHdfsFile(sqlContext: SQLContext): DataFrame
}
