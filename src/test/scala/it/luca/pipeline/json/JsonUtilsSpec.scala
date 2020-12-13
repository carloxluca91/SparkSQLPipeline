package it.luca.pipeline.json

import argonaut._
import it.luca.pipeline.exception.UnexistingPropertyException
import it.luca.pipeline.step.read.option.{CsvColumnSpecification, CsvDataframeSchema}
import it.luca.pipeline.test.AbstractJsonSpec
import it.luca.spark.sql.utils.DataTypeUtils
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.Try

class JsonUtilsSpec extends AbstractJsonSpec {

  private val jdbcDefaultUrlKey = "jdbc.default.url"
  private val jdbcDefaultDriverKey = "jdbc.default.driver.className"

  private val jsonFileSpec1 = "jsonUtilsSpec1.json"
  private val jsonFileSpec2 = "jsonUtilsSpec2.json"
  private val schemaFile = "jsonUtilsSpecSchema.json"
  override protected val testJsonFilesToDelete: Seq[String] = jsonFileSpec1 :: jsonFileSpec2 :: schemaFile :: Nil

  // Case classes for testing purposes
  private case class TestClass(jdbcUrl: String, jdbcDriver: String)
  private implicit val encodeJson: EncodeJson[TestClass] = EncodeJson.derive[TestClass]
  private implicit val decodeJson: DecodeJson[TestClass] = DecodeJson.derive[TestClass]

  private abstract class ABC(val classType: String)
  private object ABC extends DecodeJsonSubTypes[ABC] {

    implicit def decodeJson: DecodeJson[ABC] = decodeSubTypes("classType",
      "a" -> ClassA.decodeJson,
      "b" -> ClassB.decodeJson)
  }

  private case class ClassA(override val classType: String, a: Option[String]) extends ABC(classType)
  private object ClassA {

    implicit def decodeJson: DecodeJson[ClassA] = DecodeJson.derive[ClassA]
    implicit def encodeJson: EncodeJson[ClassA] = EncodeJson.derive[ClassA]
  }

  private case class ClassB(override val classType: String, b: Int) extends ABC(classType)
  private object ClassB {

    implicit def decodeJson: DecodeJson[ClassB] = DecodeJson.derive[ClassB]
    implicit def encodeJson: EncodeJson[ClassB] = EncodeJson.derive[ClassB]
  }

  "A JsonUtils object" should
    s"correctly interpolate a .json string against a ${className[PropertiesConfiguration]} object " +
      s"if all keys are defined" in {

    (jdbcDefaultUrlKey :: jdbcDefaultDriverKey :: Nil) foreach {k => assert(jobProperties.containsKey(k))}
    val expectedUrl = jobProperties.getString(jdbcDefaultUrlKey)
    val expectedDriver = jobProperties.getString(jdbcDefaultDriverKey)

    // Define a case class holding such property keys and write it as .json file
    val testClassInstance = TestClass(s"$${$jdbcDefaultUrlKey}", s"$${$jdbcDefaultDriverKey}")
    writeAsJsonFileInTestResources(testClassInstance, jsonFileSpec1)

    // Decode the json file back to a case class and assert a correct interpolation
    val testClassDecodedInstance = JsonUtils.decodeAndInterpolateJsonFile[TestClass](asTestResource(jsonFileSpec1), jobProperties)
    assert(testClassDecodedInstance.jdbcUrl == expectedUrl)
    assert(testClassDecodedInstance.jdbcDriver == expectedDriver)
    toJsonString(testClassDecodedInstance)
  }

  it should s"throw a ${className[UnexistingPropertyException]} if one key is not defined" in {

    val strangeProperty = "a.strange.property"
    assert(!jobProperties.containsKey(strangeProperty))

    // Define a case class holding such property keys and write it as .json file
    val testClassInstance = TestClass(s"$${$strangeProperty}", s"$${$jdbcDefaultDriverKey}")
    writeAsJsonFileInTestResources(testClassInstance, jsonFileSpec2)

    // Decode the json file back to a case class and assert a correct interpolation
    val tryToDecodeAs: Try[TestClass] = Try {
      JsonUtils.decodeAndInterpolateJsonFile[TestClass](asTestResource(jsonFileSpec2), jobProperties)
    }

    assert(tryToDecodeAs.isFailure)
    val exception = tryToDecodeAs.failed.get
    assert(exception.isInstanceOf[UnexistingPropertyException])
  }

  it should s"correctly parse an abstract class from its subclasses" in {

    val classAJsonString: String = toJsonString(ClassA("a", Some("a")))
    val classBJsonString: String = toJsonString(ClassB("b", 1))
    val first = JsonUtils.decodeJsonString[ABC](classAJsonString)
    assert(first.isInstanceOf[ClassA])
    val firstAsClassA = first.asInstanceOf[ClassA]
    assert(firstAsClassA.a.nonEmpty)

    val second = JsonUtils.decodeJsonString[ABC](classBJsonString)
    assert(second.isInstanceOf[ClassB])
  }

  it should s"be able to parse a suitable .json string as a ${className[StructType]} object" in {

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
    writeAsJsonFileInTestResources[CsvDataframeSchema](inputCsvSchema, schemaFile)

    // Decode json file and perform assertions
    val decodedCsvSchema: StructType = JsonUtils.fromSchemaToStructType(asTestResource(schemaFile))
    assert(decodedCsvSchema.size == csvColumnSpecifications.size)
    decodedCsvSchema.zip(csvColumnSpecifications) foreach { t =>

      val (actual, expected): (StructField, CsvColumnSpecification) = t
      assert(actual.name == expected.name)
      assert(actual.nullable == expected.nullable)
      assert(actual.dataType == DataTypeUtils.dataType(expected.dataType))
    }
  }
}
