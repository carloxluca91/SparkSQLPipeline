package it.luca.pipeline.step.transform.option

abstract class SingleSourceTransformationOptions(override val transformationType: String,
                                                 val inputSourceId: String)
  extends TransformationOptions(transformationType)