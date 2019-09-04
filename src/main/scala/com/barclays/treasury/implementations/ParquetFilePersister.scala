package com.barclays.treasury.implementations

import java.io.File

import org.apache.spark.sql.DataFrame

case class ParquetFilePersister(path: String){

   def persistHdfsFile(dataFrame: DataFrame): File = {
    dataFrame.write.mode("Overwrite").parquet(path)
    return FileProcessor().getLastGeneratedFile(path)
  }
}
