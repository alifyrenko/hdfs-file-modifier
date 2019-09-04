import com.barclays.treasury.modifier.{AvscSchemaReader, DataFrameModifier}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class DataFrameModifierTest extends FlatSpec with BeforeAndAfter with Matchers{

  // Arrange
  private val sparkSession = new SparkSession.Builder().config("spark.master", "local").getOrCreate()
  private val sqlContext = sparkSession.sqlContext
  private val dataFrameFromCsv = sqlContext.read.format("csv")
                                                .option("header", "true")
                                                .option("delimiter", "|")
                                                .load("src/test/resources/csv/meat_up_test_file.csv")

  private val dataFrame = dataFrameFromCsv.withColumn("going", dataFrameFromCsv("going").cast(IntegerType))
  private val inputDataFrame = dataFrame.withColumn("value", dataFrameFromCsv("value").cast(IntegerType))

  private val newSchemaStructure = AvscSchemaReader("src/test/resources/schema/meatUpSchemaTest.avsc").readSchema()
  private val initialRowCount = inputDataFrame.count()
  private val initialCheckSum =  inputDataFrame.groupBy().sum("value").first.get(0)

  // Act
  private val outputDataFrame = DataFrameModifier().modifySchema(inputDataFrame, newSchemaStructure, sqlContext)
  private val actualDataType = outputDataFrame.schema.filter(f => f.name == "going").head.dataType
  private val newField = outputDataFrame.schema.filter(f => f.name == "MyNewField").head.name
  private val deletedField = outputDataFrame.schema.filter(f => f.name == "organizer")
  private val outputRowCount = outputDataFrame.count()
  private val outputCheckSum =  outputDataFrame.groupBy().sum("value").first.get(0)

  // Assert
  "DataType after casting" should "be StringType" in {
    actualDataType should be (StringType)
  }
  "New field 'MyNewField'" should "appear with null value" in {
    newField should be ("MyNewField")
  }
  "Field 'organizer'" should "disappear" in {
    deletedField should be (Nil)
  }
  "Initial RowCount" should "be equal to outcome RowCount" in {
    outputRowCount should be (initialRowCount)
  }
  "Initial CheckSum by 'value' field" should "be equal to outcome CheckSum" in {
    outputCheckSum should be (initialCheckSum)
  }
}
