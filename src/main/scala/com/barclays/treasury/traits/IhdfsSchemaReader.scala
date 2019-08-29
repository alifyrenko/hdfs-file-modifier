package com.barclays.treasury.traits

import org.apache.spark.sql.types.StructType

trait IhdfsSchemaReader {
  def readSchema(): StructType
}
