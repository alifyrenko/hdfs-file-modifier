package com.barclays.treasury.traits

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

trait IDataFrameModifier {
  def modifySchema(inputDataFrame: DataFrame, schema: StructType, sqlContext: SQLContext): DataFrame
}
