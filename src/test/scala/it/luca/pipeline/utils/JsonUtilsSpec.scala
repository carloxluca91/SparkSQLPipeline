package it.luca.pipeline.utils

import argonaut.{DecodeJson, EncodeJson}
import it.luca.pipeline.test.AbstractJsonSpec

class JsonUtilsSpec extends AbstractJsonSpec {

  private final val testJsonFile = "test.json"
  private case class TestClass(jdbcUrl: String, jdbcDriver: String)
  private implicit val encodeJson: EncodeJson[TestClass] = EncodeJson.derive[TestClass]
  private implicit val decodeJson: DecodeJson[TestClass] = DecodeJson.derive[TestClass]

  "A JsonUtils object" should
    s"correctly interpolate a .json string using a ${className[JobProperties]} object" in {

    // Assert that both property keys are within test properties file
    val jdbcDefaultUrlKey = "jdbc.default.url"
    val jdbcDefaultDriverKey = "jdbc.default.driver.className"
    (jdbcDefaultUrlKey :: jdbcDefaultDriverKey :: Nil) foreach {k => assert(jobProperties.containsKey(k))}
    val expectedUrl = jobProperties.get(jdbcDefaultUrlKey)
    val expectedDriver = jobProperties.get(jdbcDefaultDriverKey)

    // Define a case class holding such property keys and write it as .json file
    val testClassInstance = TestClass(s"$${$jdbcDefaultUrlKey}", s"$${$jdbcDefaultDriverKey}")
    writeJsonFile(testClassInstance, testJsonFile)

    // Decode the json file back to a case class and assert a correct interpolation
    val testClassDecodedInstance = JsonUtils.decodeAndInterpolateJsonFile[TestClass](testJsonFile, jobProperties)
    assert(testClassDecodedInstance.jdbcUrl == expectedUrl)
    assert(testClassDecodedInstance.jdbcDriver == expectedDriver)
    toJsonString(testClassDecodedInstance)
  }
}
