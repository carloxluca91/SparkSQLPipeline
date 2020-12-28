package it.luca.pipeline.step.transform.option.common

abstract class MultipleSrcTransformationOptions[T](override val transformationType: String,
                                                   val inputDfIds: T)
  extends TransformationOptions(transformationType)
