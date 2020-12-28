package it.luca.pipeline.step.transform.option.common

abstract class SingleSrcTransformationOptions(override val transformationType: String,
                                              val inputDfId: String)
  extends TransformationOptions(transformationType)