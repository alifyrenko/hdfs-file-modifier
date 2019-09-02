import com.barclays.treasury.implementations.{AvscSchemaReader, DataFrameModifier, ParquetFileReader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class DataFrameModifierTest extends FlatSpec with BeforeAndAfter with Matchers{

  // Arrange
  private val sparkSession = new SparkSession.Builder().config("spark.master", "local").getOrCreate()
  private val sqlContext = sparkSession.sqlContext
  private val inputDataFrame = ParquetFileReader("src/test/resources/parquet/meetup_parquet.parquet").readHdfsFile(sqlContext)
  private val newSchemaStructure = AvscSchemaReader("src/test/resources/schema/meatUpSchemaTest.avsc").readSchema()

  // Act
  private val outputDataFrame = DataFrameModifier().modifySchema(inputDataFrame, newSchemaStructure, sqlContext)
  private val actualDataType = outputDataFrame.schema.filter(f => f.name == "going").head.dataType
  private val newField = outputDataFrame.schema.filter(f => f.name == "MyNewField").head.name
  private val deletedField = outputDataFrame.schema.filter(f => f.name == "organizer")

  // Assert
  actualDataType should be (StringType)
  newField should be ("MyNewField")
  deletedField should be (Nil)
}