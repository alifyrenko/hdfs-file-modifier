package com.barclays.treasury.modifier

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ListBuffer


case class DataFrameModifier() {

  def modifySchema(inputDataFrame: DataFrame, schema: StructType, sqlContext: SQLContext): DataFrame = {

    val outputDataFrame = sqlContext.createDataFrame(sqlContext.emptyDataFrame.toJavaRDD, schema)

    val fieldNameList = ListBuffer[String]()
    inputDataFrame.schema.fields.foreach(f => fieldNameList += f.name)

    val expression = outputDataFrame.schema.fields.map { f =>
      if (inputDataFrame.schema.fields.contains(f)) col(f.name)
      else if(fieldNameList.contains(f.name)) lit(col(f.name)).cast(f.dataType).alias(f.name)
      else lit(null).cast(f.dataType).alias(f.name)
    }

    return inputDataFrame.select(expression: _*).toDF()
  }
}