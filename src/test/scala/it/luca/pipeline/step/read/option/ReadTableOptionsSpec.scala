package it.luca.pipeline.step.read.option

import argonaut._
import it.luca.pipeline.json.{JsonField, JsonValue}
import it.luca.pipeline.step.common.JDBCOptions
import it.luca.pipeline.test.JsonSpec
import it.luca.pipeline.utils.JsonUtils

class ReadTableOptionsSpec extends JsonSpec {

  private final val hiveSource = JsonValue.HiveSourceOrDestination.value
  private final val jdbcSource = JsonValue.JDBCSourceOrDestination.value
  private final val jdbcTableSrcOptionsApply: Option[String] => ReadJDBCTableOptions = opt => {
    val jdbcOptions =  JDBCOptions("url", "driver", "user", "pw", opt)
    ReadJDBCTableOptions(jdbcSource, "db", "table", jdbcOptions)
  }

  s"A ${className[ReadOptions]} object" should
    s"be parsed as a ${className[ReadHiveTableOptions]} object " +
      s"when ${JsonField.SourceType.label} = '$hiveSource'" in {

    implicit val encodeJson: EncodeJson[ReadHiveTableOptions] = EncodeJson.derive[ReadHiveTableOptions]
    val inputString: String = toJsonString(ReadHiveTableOptions(hiveSource, "db", "table"))

    val srcOptions = JsonUtils.decodeJsonString[ReadOptions](inputString)
    assert(srcOptions.isInstanceOf[ReadHiveTableOptions])
    val hiveTableSrcOptions = srcOptions.asInstanceOf[ReadHiveTableOptions]
    assert(hiveTableSrcOptions.dbName == "db")
    assert(hiveTableSrcOptions.tableName == "table")
  }

  it should s"be correctly parsed as a ${className[ReadJDBCTableOptions]} object " +
    s"when ${JsonField.SourceType.label} = '$jdbcSource'" in {

    implicit val encodeJsonJDBCOptions: EncodeJson[JDBCOptions] = EncodeJson.derive[JDBCOptions]
    implicit val encodeJson: EncodeJson[ReadJDBCTableOptions] = EncodeJson.derive[ReadJDBCTableOptions]
    val inputString: String = toJsonString(jdbcTableSrcOptionsApply(Some("ssl")))

    val srcOptions = JsonUtils.decodeJsonString[ReadOptions](inputString)
    assert(srcOptions.isInstanceOf[ReadJDBCTableOptions])
    val jdbcTableSrcOptions = srcOptions.asInstanceOf[ReadJDBCTableOptions]
    assert(jdbcTableSrcOptions.jdbcOptions.jdbcUseSSL.nonEmpty)
  }

  it should s"be correctly parsed as a ${className[ReadJDBCTableOptions]} object " +
    s"when ${JsonField.SourceType.label} = '$jdbcSource' " +
    s"even if '${JsonField.JDBCUseSSL.label}' field is missing" in {

    implicit val encodeJsonJDBCOptions: EncodeJson[JDBCOptions] = EncodeJson.jencode4L((j: JDBCOptions) =>
      (j.jdbcUrl, j.jdbcDriver, j.jdbcUser, j.jdbcPassword))(JsonField.JDBCUrl.label,
        JsonField.JDBCDriver.label,
        JsonField.JDBCUser.label,
        JsonField.JDBCPassword.label)

    implicit val encodeJsonReadJDBCTableOptions: EncodeJson[ReadJDBCTableOptions] = EncodeJson.derive[ReadJDBCTableOptions]
    val inputString = toJsonString(jdbcTableSrcOptionsApply(None))
    val srcOptions = JsonUtils.decodeJsonString[ReadOptions](inputString)
    assert(srcOptions.isInstanceOf[ReadJDBCTableOptions])
    val jdbcTableSrcOptions = srcOptions.asInstanceOf[ReadJDBCTableOptions]
    assert(jdbcTableSrcOptions.jdbcOptions.jdbcUseSSL.isEmpty)
  }
}
