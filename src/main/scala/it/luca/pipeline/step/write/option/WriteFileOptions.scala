package it.luca.pipeline.step.write.option

abstract class WriteFileOptions(override val destinationType: String,
                                override val saveMode: String,
                                val path: String,
                                override val partitionBy: Option[List[String]],
                                override val coalesce: Option[Int])

  extends WriteOptions(destinationType, saveMode, partitionBy, coalesce)