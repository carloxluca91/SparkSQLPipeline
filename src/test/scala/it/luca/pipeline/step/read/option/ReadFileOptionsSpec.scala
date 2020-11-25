package it.luca.pipeline.step.read.option

import argonaut.EncodeJson
import it.luca.pipeline.json.{JsonField, JsonValue}
import it.luca.pipeline.test.AbstractJsonSpec
import it.luca.pipeline.utils.JsonUtils

class ReadFileOptionsSpec extends AbstractJsonSpec {

  private final val csvSource = JsonValue.CsvSourceOrDestination.value
  private final val csvSrcOptionsApply: (Option[String], Option[String]) => ReadCsvOptions
  = ReadCsvOptions(JsonValue.CsvSourceOrDestination.value, "path", "schema", _: Option[String], _: Option[String])

  s"A ${className[ReadOptions]} object" should
    s"be parsed as ${className[ReadCsvOptions]} " +
      s"when ${JsonField.SourceType.label} = '$csvSource'" in {

    implicit val encodeJson: EncodeJson[ReadCsvOptions] = EncodeJson.derive[ReadCsvOptions]
    val inputString: String = toJsonString[ReadCsvOptions](csvSrcOptionsApply(Some(","), Some("header")))

    val srcOptions = JsonUtils.decodeJsonString[ReadCsvOptions](inputString)
    assert(srcOptions.isInstanceOf[ReadCsvOptions])
    val csvSrcOptions = srcOptions.asInstanceOf[ReadCsvOptions]
    assert(csvSrcOptions.header.nonEmpty)
    assert(csvSrcOptions.separator.nonEmpty)
  }

  it should s"be parsed as ${className[ReadCsvOptions]} " +
    s"when ${JsonField.SourceType.label} = '$csvSource' " +
    s"even if both '${JsonField.Header.label}' and '${JsonField.Separator.label}' field(s) are missing" in {

    implicit val encodeJson: EncodeJson[ReadCsvOptions] = EncodeJson.jencode3L((c: ReadCsvOptions) =>
      (c.sourceType, c.path, c.schemaFile))(
      JsonField.SourceType.label, JsonField.Path.label, JsonField.SchemaFile.label)
    val inputString: String = toJsonString[ReadCsvOptions](csvSrcOptionsApply(None, None))

    val srcOptions = JsonUtils.decodeJsonString[ReadCsvOptions](inputString)
    assert(srcOptions.isInstanceOf[ReadCsvOptions])
    val csvSrcOptions = srcOptions.asInstanceOf[ReadCsvOptions]
    assert(csvSrcOptions.header.isEmpty)
    assert(csvSrcOptions.separator.isEmpty)
  }
}
