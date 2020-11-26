package it.luca.pipeline.step.transform.option

case class JoinTransformationOptions(override val transformationType: String,
                                     leftDataframeId: String,
                                     rightDataframeId: String,
                                     joinType: String,
                                     joinCondition: String)
  extends TransformationOptions(transformationType)
