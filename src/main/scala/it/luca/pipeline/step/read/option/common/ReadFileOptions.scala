package it.luca.pipeline.step.read.option.common

abstract class ReadFileOptions(override val sourceType: String,
                               val path: String)
  extends ReadOptions(sourceType)