package it.luca.pipeline.step.write.common

abstract class WriteTableOptions(override val destinationType: String,
                                 val dbName: String,
                                 val tableName: String,
                                 val createDbIfNotExists: Option[String],
                                 override val saveMode: String,
                                 override val partitionBy: Option[List[String]],
                                 override val coalesce: Option[Int])

  extends WriteOptions(destinationType, saveMode, partitionBy, coalesce)
