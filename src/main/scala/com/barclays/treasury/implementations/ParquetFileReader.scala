package com.barclays.treasury.implementations

import org.apache.spark.sql.{DataFrame, SQLContext}

case class ParquetFileReader(path: String) {

  def readHdfsFile(sqlContext: SQLContext): DataFrame = {
    return sqlContext.read.parquet(path)
  }
}
