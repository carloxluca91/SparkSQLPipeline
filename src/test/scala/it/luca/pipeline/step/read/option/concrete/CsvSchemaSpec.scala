package it.luca.pipeline.step.read.option.concrete

import argonaut.EncodeJson
import it.luca.pipeline.json.{JsonUtils, JsonValue}
import it.luca.pipeline.test.AbstractJsonSpec
import it.luca.spark.sql.utils.DataTypeUtils
import org.apache.spark.sql.types.StructType

class CsvSchemaSpec extends AbstractJsonSpec {

  s"A ${className[CsvSchema]} object" should
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

    val csvSchema = CsvSchema("DataFrame description", csvColumnSpecifications)
    implicit val encodeJsonColumn: EncodeJson[CsvColumnSpecification] = EncodeJson.derive[CsvColumnSpecification]
    implicit val encodeJsonSchema: EncodeJson[CsvSchema] = EncodeJson.derive[CsvSchema]

    val jsonString: String = toJsonString(csvSchema)
    val decodedCsvSchema = JsonUtils.decodeJsonString[CsvSchema](jsonString)
    val structType: StructType = CsvSchema.toStructType(decodedCsvSchema)
    structType
      .fields
      .zip(decodedCsvSchema.columns) foreach { case (field, specification) =>

      assertResult(field.name)(specification.name)
      assertResult(field.dataType)(DataTypeUtils.dataType(specification.dataType))
      assertResult(field.nullable)(specification.nullable)
    }
  }
}
