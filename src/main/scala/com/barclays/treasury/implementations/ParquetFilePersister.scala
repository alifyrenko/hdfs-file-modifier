package com.barclays.treasury.implementations

import com.barclays.treasury.traits.IhdfsFilePersister
import org.apache.spark.sql.DataFrame

case class ParquetFilePersister(path: String) extends IhdfsFilePersister{

  override def persistHdfsFile(dataFrame: DataFrame): Unit = {
    dataFrame.write.mode("Append").parquet(path)
  }
}
