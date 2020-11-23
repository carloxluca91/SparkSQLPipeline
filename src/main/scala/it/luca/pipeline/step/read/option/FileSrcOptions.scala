package it.luca.pipeline.step.read.option

abstract class FileSrcOptions(override val sourceType: String, val path: String)
  extends SrcOptions(sourceType)