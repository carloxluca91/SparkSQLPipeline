package it.luca.pipeline.step.transform.option

abstract class JoinTransformationOptions(override val transformationType: String,
                                         val inputSourceIds: List[String])
  extends TransformationOptions(transformationType)
