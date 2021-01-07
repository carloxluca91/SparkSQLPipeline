package it.luca.pipeline.step.read

import argonaut._, Argonaut._
import it.luca.pipeline.exception.ReadHiveTableOptionsException
import it.luca.pipeline.json.JsonUtils
import it.luca.pipeline.test.AbstractJsonSpec

class ReadHiveTableOptionsSpec extends AbstractJsonSpec {

  s"A ${className[ReadHiveTableOptions]} object" should
    s"throw a ${className[ReadHiveTableOptionsException]} when both 'tableName' and 'sqlQuery are undefined'" in {

    val startingOptions = ReadHiveTableOptions("hive", None, None)
    implicit def encodeJson: EncodeJson[ReadHiveTableOptions] = EncodeJson.derive[ReadHiveTableOptions]

    val jsonStringWithDroppedNone = startingOptions
      .asJson.pretty(PrettyParams.spaces4.copy(dropNullKeys = true))

    val decodeOptions = JsonUtils.decodeJsonString[ReadHiveTableOptions](jsonStringWithDroppedNone)
    assertThrows[ReadHiveTableOptionsException](HiveTableReader.read(decodeOptions, null))
  }
}
