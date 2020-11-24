package it.luca.pipeline.step.read.option

abstract class ReadFileOptions(override val sourceType: String, val path: String)
  extends ReadOptions(sourceType)