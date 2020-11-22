package it.luca.pipeline.step.read

import argonaut.Argonaut.jdecode5L
import argonaut.DecodeJson
import it.luca.pipeline.json.JsonField

sealed abstract class FileSrcOptions(override val sourceType: String, val path: String)
  extends SrcOptions(sourceType)

case class CsvSrcOptions(override val sourceType: String,
                         override val path: String,
                         schemaFile: String,
                         separatorOpt: Option[String],
                         headerOpt: Option[String])
  extends FileSrcOptions(sourceType, path)

object CsvSrcOptions {

  implicit def csvSrcOptionDecodeJson: DecodeJson[CsvSrcOptions] =
    jdecode5L(CsvSrcOptions.apply)(JsonField.SourceType.label,
      JsonField.Path.label,
      JsonField.SchemaFile.label,
      JsonField.Separator.label,
      JsonField.Header.label)
}