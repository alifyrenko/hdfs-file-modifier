package com.barclays.treasury.implementations

import java.io.File

import com.barclays.treasury.traits.IhdfsSchemaReader
import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType

case class AvscSchemaReader(path: String) extends IhdfsSchemaReader {

  override def readSchema(): StructType = {
    val schema = new Schema.Parser().parse(new File(path))
    return SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
  }
}
