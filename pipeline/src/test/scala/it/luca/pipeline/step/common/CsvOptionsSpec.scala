package it.luca.pipeline.step.common

import argonaut.EncodeJson
import it.luca.pipeline.json.{JsonUtils, JsonValue}
import it.luca.pipeline.test.AbstractJsonSpec
import it.luca.spark.sql.types.DataTypeUtils
import org.apache.spark.sql.types.StructType

class CsvOptionsSpec extends AbstractJsonSpec {

  s"A ${className[CsvOptions]} object" should s"be able to parse a suitable .json string as a ${className[StructType]} object" in {

    // Initialize some column objects
    val columnSpecificationMap: Map[String, String] = Map(
      "col1" -> JsonValue.String,
      "col2" -> JsonValue.Date)

    // Initialize a schema defined by such column objects
    val csvColumnSpecifications: List[CsvColumnSpecification] = columnSpecificationMap
      .map(t => {
        val (name, dataType) = t
        CsvColumnSpecification(name, dataType)
      }).toList

    val csvSchema = CsvOptions(csvColumnSpecifications, Some(";"), None)
    implicit val encodeJsonColumn: EncodeJson[CsvColumnSpecification] = EncodeJson.derive[CsvColumnSpecification]
    implicit val encodeJsonSchema: EncodeJson[CsvOptions] = EncodeJson.derive[CsvOptions]

    val jsonString: String = toJsonString(csvSchema)
    val decodedCsvOptions = JsonUtils.decodeJsonString[CsvOptions](jsonString)
    val structType: StructType = decodedCsvOptions.schemaAsStructType
    structType
      .fields
      .zip(decodedCsvOptions.schema) foreach { case (field, specification) =>

      assertResult(field.name)(specification.name)
      assertResult(field.dataType)(DataTypeUtils.dataType(specification.dataType))
    }
  }
}
