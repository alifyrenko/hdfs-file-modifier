package com.barclays.treasury.traits

import org.apache.spark.sql.DataFrame

trait IhdfsFilePersister {
  def persistHdfsFile(dataFrame: DataFrame)
}
