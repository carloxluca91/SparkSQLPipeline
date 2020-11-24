package it.luca.pipeline.step.read.option

abstract class ReadTableOptions(override val sourceType: String,
                                val dbName: String,
                                val tableName: String)
  extends ReadOptions(sourceType)
