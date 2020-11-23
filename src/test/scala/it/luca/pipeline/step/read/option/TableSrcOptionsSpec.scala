package it.luca.pipeline.step.read.option

import argonaut._
import it.luca.pipeline.json.{JsonField, JsonValue}
import it.luca.pipeline.test.JsonUnitTest
import it.luca.pipeline.utils.Json

class TableSrcOptionsSpec extends JsonUnitTest {

  private final val hiveSource = JsonValue.HiveSource.value
  private final val jdbcSource = JsonValue.JDBCSource.value
  private final val jdbcTableSrcOptionsApply: Option[String] => JDBCTableSrcOptions
  = JDBCTableSrcOptions(jdbcSource, "db", "table", "url", "driver", "user", "pw", _: Option[String])

  s"A ${className[SrcOptions]} object" should
    s"be parsed as a ${className[HiveTableSrcOptions]} object " +
      s"when ${JsonField.SourceType.label} = '$hiveSource'" in {

    implicit val encodeJson: EncodeJson[HiveTableSrcOptions] = EncodeJson.derive[HiveTableSrcOptions]
    val inputString: String = toJsonString(HiveTableSrcOptions(hiveSource, "db", "table"))

    val srcOptions = Json.decodeJsonString[SrcOptions](inputString)
    assert(srcOptions.isInstanceOf[HiveTableSrcOptions])
    val hiveTableSrcOptions = srcOptions.asInstanceOf[HiveTableSrcOptions]
    assert(hiveTableSrcOptions.dbName == "db")
    assert(hiveTableSrcOptions.tableName == "table")
  }

  it should s"be correctly parsed as a ${className[JDBCTableSrcOptions]} object " +
    s"when ${JsonField.SourceType.label} = '$jdbcSource'" in {

    implicit val encodeJson: EncodeJson[JDBCTableSrcOptions] = EncodeJson.derive[JDBCTableSrcOptions]
    val inputString: String = toJsonString(jdbcTableSrcOptionsApply(Some("ssl")))

    val srcOptions = Json.decodeJsonString[SrcOptions](inputString)
    assert(srcOptions.isInstanceOf[JDBCTableSrcOptions])
    val jdbcTableSrcOptions = srcOptions.asInstanceOf[JDBCTableSrcOptions]
    assert(jdbcTableSrcOptions.jdbcUseSSL.nonEmpty)
  }

  it should s"be correctly parsed as a ${className[JDBCTableSrcOptions]} object " +
    s"when ${JsonField.SourceType.label} = '$jdbcSource' " +
    s"even if '${JsonField.JDBCUseSSL.label}' field is missing" in {

    implicit val encodeJson: EncodeJson[JDBCTableSrcOptions] = EncodeJson.jencode7L((j: JDBCTableSrcOptions) =>
      (j.sourceType, j.dbName, j.tableName, j.jdbcUrl, j.jdbcDriver, j.jdbcUser, j.jdbcPassword))(
      JsonField.SourceType.label,
      JsonField.DbName.label,
      JsonField.TableName.label,
      JsonField.JDBCUrl.label,
      JsonField.JDBCDriver.label,
      JsonField.JDBCUser.label,
      JsonField.JDBCPassword.label)

    val inputString = toJsonString(jdbcTableSrcOptionsApply(None))
    val srcOptions = Json.decodeJsonString[SrcOptions](inputString)
    assert(srcOptions.isInstanceOf[JDBCTableSrcOptions])
    val jdbcTableSrcOptions = srcOptions.asInstanceOf[JDBCTableSrcOptions]
    assert(jdbcTableSrcOptions.jdbcUseSSL.isEmpty)
  }
}
