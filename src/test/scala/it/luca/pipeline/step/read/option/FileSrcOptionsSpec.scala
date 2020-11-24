package it.luca.pipeline.step.read.option

import argonaut.EncodeJson
import it.luca.pipeline.json.{JsonField, JsonValue}
import it.luca.pipeline.test.JsonSpec
import it.luca.pipeline.utils.JsonUtils

class FileSrcOptionsSpec extends JsonSpec {

  private final val csvSource = JsonValue.CsvSource.value
  private final val csvSrcOptionsApply: (Option[String], Option[String]) => CsvSrcOptions
  = CsvSrcOptions(JsonValue.CsvSource.value, "path", "schema", _: Option[String], _: Option[String])

  s"A ${className[SrcOptions]} object" should
    s"be parsed as ${className[CsvSrcOptions]} " +
      s"when ${JsonField.SourceType.label} = '$csvSource'" in {

    implicit val encodeJson: EncodeJson[CsvSrcOptions] = EncodeJson.derive[CsvSrcOptions]
    val inputString: String = toJsonString[CsvSrcOptions](csvSrcOptionsApply(Some(","), Some("header")))

    val srcOptions = JsonUtils.decodeJsonString[CsvSrcOptions](inputString)
    assert(srcOptions.isInstanceOf[CsvSrcOptions])
    val csvSrcOptions = srcOptions.asInstanceOf[CsvSrcOptions]
    assert(csvSrcOptions.header.nonEmpty)
    assert(csvSrcOptions.separator.nonEmpty)
  }

  it should s"be parsed as ${className[CsvSrcOptions]} " +
    s"when ${JsonField.SourceType.label} = '$csvSource' " +
    s"even if both '${JsonField.Header.label}' and '${JsonField.Separator.label}' field(s) are missing" in {

    implicit val encodeJson: EncodeJson[CsvSrcOptions] = EncodeJson.jencode3L((c: CsvSrcOptions) =>
      (c.sourceType, c.path, c.schemaFile))(
      JsonField.SourceType.label, JsonField.Path.label, JsonField.SchemaFile.label)
    val inputString: String = toJsonString[CsvSrcOptions](csvSrcOptionsApply(None, None))

    val srcOptions = JsonUtils.decodeJsonString[CsvSrcOptions](inputString)
    assert(srcOptions.isInstanceOf[CsvSrcOptions])
    val csvSrcOptions = srcOptions.asInstanceOf[CsvSrcOptions]
    assert(csvSrcOptions.header.isEmpty)
    assert(csvSrcOptions.separator.isEmpty)
  }
}
