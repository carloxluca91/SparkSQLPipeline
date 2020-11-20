package it.luca.pipeline.step.read

sealed abstract class FileSrcOptions(override val sourceType: String, val path: String)
  extends SrcOptions(sourceType)

case class CsvSrcOptions(override val sourceType: String,
                         override val path: String,
                         schemaFile: String,
                         separator: Option[String],
                         header: Option[Boolean])
  extends FileSrcOptions(sourceType, path)