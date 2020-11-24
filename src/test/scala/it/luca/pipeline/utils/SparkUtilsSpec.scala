package it.luca.pipeline.utils

import java.io.{BufferedWriter, File, FileWriter}

import argonaut.EncodeJson
import it.luca.pipeline.json.JsonValue
import it.luca.pipeline.step.read.option.{CsvColumnSpecification, CsvDataframeSchema}
import it.luca.pipeline.test.JsonSpec
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class SparkUtilsSpec extends JsonSpec {

  private final val SchemaFilePath = "csv_schema.json"
  private final val UndefinedType = "undefined"

  s"A SparkUtils object" should
    s"return default datatype (${DataTypes.StringType}) " +
    s"when a datatype is undefined" in {

    assert(SparkUtils.asSparkDataType(UndefinedType) == DataTypes.StringType)
  }

  it should s"be able to parse a suitable .json string as a ${className[StructType]} object" in {

    // Initialize some column objects
    val actualNullableFlag = false
    val columnSpecificationMap: Map[String, (String, String, Boolean)] = Map(
      "col1" -> ("first", JsonValue.StringType.value, actualNullableFlag),
      "col2" -> ("second", JsonValue.DateType.value, actualNullableFlag))

    // Initialize a schema defined by such column objects
    val csvColumnSpecifications: List[CsvColumnSpecification] = columnSpecificationMap
      .map(t => {

        val (name, (description, dataType, nullable)) = t
        CsvColumnSpecification(name, description, dataType, nullable)
      }).toList

    val inputCsvSchema = CsvDataframeSchema("df1", "first dataframe", csvColumnSpecifications)

    // Encode as json and write on a file
    implicit val encodeJsonColumn: EncodeJson[CsvColumnSpecification] = EncodeJson.derive[CsvColumnSpecification]
    implicit val encodeJsonSchema: EncodeJson[CsvDataframeSchema] = EncodeJson.derive[CsvDataframeSchema]
    val jsonString: String = toJsonString(inputCsvSchema)
    val bufferedWriter = new BufferedWriter(new FileWriter(new File(SchemaFilePath)))
    bufferedWriter.write(jsonString)
    bufferedWriter.close()

    // Decode json file and perform assertions
    val decodedCsvSchema: StructType = SparkUtils.fromSchemaToStructType(SchemaFilePath)
    assert(decodedCsvSchema.size == csvColumnSpecifications.size)
    decodedCsvSchema.zip(csvColumnSpecifications) foreach { t =>

      val (actual, expected): (StructField, CsvColumnSpecification) = t
      assert(actual.name == expected.name)
      assert(actual.nullable == expected.nullable)
      assert(actual.dataType == SparkUtils.asSparkDataType(expected.dataType))
    }

    new File(SchemaFilePath).delete()
  }
}
