package com.barclays.treasury.implementations

import java.io.File

import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType

case class AvscSchemaReader(avscSchemaPath: String) {

  def readSchema(): StructType = {
    val schemaFile = new File(avscSchemaPath)
    val schema = new Schema.Parser().parse(schemaFile)
    return SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
  }
}
