package it.luca.pipeline.spark

import argonaut.EncodeJson
import it.luca.pipeline.json.JsonValue
import it.luca.pipeline.step.read.option.{CsvColumnSpecification, CsvDataframeSchema}
import it.luca.pipeline.test.AbstractJsonSpec
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class SparkUtilsSpec extends AbstractJsonSpec {

  private val SchemaFileName = "sparkUtilsSpec.json"
  override protected val testJsonFilesToDelete: Seq[String] = SchemaFileName :: Nil
  private val UndefinedType = "undefined"

  s"A SparkUtils object" should
    s"return default datatype (${DataTypes.StringType}) " +
    s"when a datatype is undefined" in {

    assert(SparkUtils.asSparkDataType(UndefinedType) == DataTypes.StringType)
  }

  it should
    s"be able to parse a suitable .json string as a ${className[StructType]} object" in {

    // Initialize some column objects
    val expectedFlag = false
    val columnSpecificationMap: Map[String, (String, String, Boolean)] = Map(
      "col1" -> ("first", JsonValue.StringType.value, expectedFlag),
      "col2" -> ("second", JsonValue.DateType.value, expectedFlag))

    // Initialize a schema defined by such column objects
    val csvColumnSpecifications: List[CsvColumnSpecification] = columnSpecificationMap
      .map(t => {

        val (name, (description, dataType, nullable)) = t
        CsvColumnSpecification(name, description, dataType, nullable)
      }).toList

    val inputCsvSchema = CsvDataframeSchema("df1", "first dataframe", csvColumnSpecifications)

    // Write on a .json file
    implicit val encodeJsonColumn: EncodeJson[CsvColumnSpecification] = EncodeJson.derive[CsvColumnSpecification]
    implicit val encodeJsonSchema: EncodeJson[CsvDataframeSchema] = EncodeJson.derive[CsvDataframeSchema]
    writeAsJsonFileInTestResources[CsvDataframeSchema](inputCsvSchema, SchemaFileName)

    // Decode json file and perform assertions
    val decodedCsvSchema: StructType = SparkUtils.fromSchemaToStructType(asTestResource(SchemaFileName))
    assert(decodedCsvSchema.size == csvColumnSpecifications.size)
    decodedCsvSchema.zip(csvColumnSpecifications) foreach { t =>

      val (actual, expected): (StructField, CsvColumnSpecification) = t
      assert(actual.name == expected.name)
      assert(actual.nullable == expected.nullable)
      assert(actual.dataType == SparkUtils.asSparkDataType(expected.dataType))
    }
  }
}
