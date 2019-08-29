package com.barclays.treasury.implementations

import com.barclays.treasury.traits.IhdfsFileReader
import org.apache.spark.sql.{DataFrame, SQLContext}

case class ParquetFileReader(path: String) extends IhdfsFileReader{

  override def readHdfsFile(sqlContext: SQLContext): DataFrame = {
    return sqlContext.read.parquet(path)
  }
}
