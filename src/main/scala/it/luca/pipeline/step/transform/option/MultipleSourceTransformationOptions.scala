package it.luca.pipeline.step.transform.option

abstract class MultipleSourceTransformationOptions(override val transformationType: String,
                                                   val inputSourceIds: List[String])
  extends TransformationOptions(transformationType)
