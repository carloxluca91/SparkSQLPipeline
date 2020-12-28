package it.luca.pipeline.step.write.option.common

abstract class WriteFileOptions(override val destinationType: String,
                                override val saveOptions: SaveOptions,
                                val path: String)

  extends WriteOptions(destinationType, saveOptions)