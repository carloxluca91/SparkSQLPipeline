package it.luca.pipeline.step.read.common

abstract class ReadTableOptions(override val sourceType: String,
                                val dbName: String,
                                val tableName: String)
  extends ReadOptions(sourceType)
