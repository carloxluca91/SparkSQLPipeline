package it.luca.pipeline.json

import argonaut._
import it.luca.pipeline.exception.{JsonDecodingException, JsonSyntaxException, UnexistingPropertyException}
import it.luca.pipeline.test.AbstractJsonSpec
import org.apache.commons.configuration.PropertiesConfiguration

import scala.util.Try

class JsonUtilsSpec extends AbstractJsonSpec {

  private val (jdbcDefaultUrlKey, jdbcDefaultDriverKey) = ("jdbc.default.url", "jdbc.default.driver.className")
  private val (typeA, typeB) = ("typeA", "typeB")

  // Case classes for testing purposes
  private case class TestClass(jdbcUrl: String, jdbcDriver: String)
  private implicit val encodeJson: EncodeJson[TestClass] = EncodeJson.derive[TestClass]
  private implicit val decodeJson: DecodeJson[TestClass] = DecodeJson.derive[TestClass]

  private abstract class ABC(val classType: String)
  private object ABC extends DecodeJsonSubTypes[ABC] {

    implicit def decodeJson: DecodeJson[ABC] = decodeSubTypes("classType",
      typeA -> ClassA.decodeJson,
      typeB -> ClassB.decodeJson)
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

  s"A ${JsonUtils.getClass.getSimpleName} object" should
    s"throw a ${className[JsonSyntaxException]} when parsing an invalid .json string" in {

    val jsonString =
      """
        |{
        |   "classType": "typeA"
        |   "a": "aValue"
        |}
        |""".stripMargin

    a [JsonSyntaxException] should be thrownBy {
      JsonUtils.decodeJsonString[ClassA](jsonString)
    }
  }

  it should s"throw a ${className[JsonDecodingException]} when parsing a valid .json string that does not match provided decoding type" in {

    val jsonString =
      """
        |{
        |   "clazzType": "typeA",
        |   "a": "aValue"
        |}
        |""".stripMargin

    a [JsonDecodingException] should be thrownBy {
      JsonUtils.decodeJsonString[ClassA](jsonString)
    }
  }

  it should s"correctly parse an abstract class from its subclasses" in {

    val classAJsonString: String = toJsonString(ClassA(typeA, Some("a")))
    val classBJsonString: String = toJsonString(ClassB(typeB, 1))
    val first = JsonUtils.decodeJsonString[ABC](classAJsonString)
    assert(first.isInstanceOf[ClassA])
    val firstAsClassA = first.asInstanceOf[ClassA]
    assert(firstAsClassA.a.nonEmpty)

    val second = JsonUtils.decodeJsonString[ABC](classBJsonString)
    assert(second.isInstanceOf[ClassB])
  }

 it should s"correctly interpolate a .json string against a ${className[PropertiesConfiguration]} object if all keys are defined" in {

    (jdbcDefaultUrlKey :: jdbcDefaultDriverKey :: Nil) foreach {k => assert(jobProperties.containsKey(k))}
    val expectedUrl = jobProperties.getString(jdbcDefaultUrlKey)
    val expectedDriver = jobProperties.getString(jdbcDefaultDriverKey)

    // Define a case class holding such property keys and encode it as .json string and back
    val jsonString: String = toJsonString(TestClass(s"$${$jdbcDefaultUrlKey}", s"$${$jdbcDefaultDriverKey}"))
    val testClassDecodedInstance = JsonUtils.decodeAndInterpolateJsonString[TestClass](jsonString, jobProperties)
    assert(testClassDecodedInstance.jdbcUrl == expectedUrl)
    assert(testClassDecodedInstance.jdbcDriver == expectedDriver)
    toJsonString(testClassDecodedInstance)
  }

  it should s"throw a ${className[UnexistingPropertyException]} if one key is not defined" in {

    val strangeProperty = "a.strange.property"
    assert(!jobProperties.containsKey(strangeProperty))

    // Define a case class holding such property keys and write it as .json file
    val jsonString: String = toJsonString(TestClass(s"$${$strangeProperty}", s"$${$jdbcDefaultDriverKey}"))
    val tryToDecodeAs: Try[TestClass] = Try {
      JsonUtils.decodeAndInterpolateJsonString[TestClass](jsonString, jobProperties)
    }

    assert(tryToDecodeAs.isFailure)
    val exception = tryToDecodeAs.failed.get
    assert(exception.isInstanceOf[UnexistingPropertyException])
  }
}
