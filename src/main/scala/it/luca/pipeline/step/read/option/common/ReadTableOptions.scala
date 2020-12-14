package it.luca.pipeline.step.read.option.common

abstract class ReadTableOptions(override val sourceType: String,
                                val dbName: String,
                                val tableName: String)
  extends ReadOptions(sourceType)
