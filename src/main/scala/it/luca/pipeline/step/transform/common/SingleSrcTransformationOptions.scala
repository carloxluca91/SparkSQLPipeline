package it.luca.pipeline.step.transform.common

abstract class SingleSrcTransformationOptions(override val transformationType: String,
                                              val inputSourceId: String)
  extends TransformationOptions(transformationType)