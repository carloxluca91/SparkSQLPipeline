package it.luca.pipeline.json

import argonaut.{DecodeJson, EncodeJson}
import it.luca.pipeline.exception.UnexistingPropertyException
import it.luca.pipeline.test.AbstractJsonSpec
import org.apache.commons.configuration.PropertiesConfiguration

import scala.util.Try

class JsonUtilsSpec extends AbstractJsonSpec {

  private val jdbcDefaultUrlKey = "jdbc.default.url"
  private val jdbcDefaultDriverKey = "jdbc.default.driver.className"

  private val jsonFileSpec1 = "jsonUtilsSpec1.json"
  private val jsonFileSpec2 = "jsonUtilsSpec2.json"
  override protected val testJsonFilesToDelete: Seq[String] = jsonFileSpec1 :: jsonFileSpec2 :: Nil

  // Case class for testing purposes
  private case class TestClass(jdbcUrl: String, jdbcDriver: String)
  private implicit val encodeJson: EncodeJson[TestClass] = EncodeJson.derive[TestClass]
  private implicit val decodeJson: DecodeJson[TestClass] = DecodeJson.derive[TestClass]

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

  it should
    s"throw a ${className[UnexistingPropertyException]} if one key is not defined" in {

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
}