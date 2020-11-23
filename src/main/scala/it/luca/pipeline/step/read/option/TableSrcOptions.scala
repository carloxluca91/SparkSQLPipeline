package it.luca.pipeline.step.read.option

abstract class TableSrcOptions(override val sourceType: String,
                                      val dbName: String,
                                      val tableName: String)
  extends SrcOptions(sourceType)
