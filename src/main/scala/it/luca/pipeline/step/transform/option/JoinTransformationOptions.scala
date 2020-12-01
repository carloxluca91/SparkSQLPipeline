package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.common.TransformationOptions

case class JoinTransformationOptions(override val transformationType: String,
                                     joinType: String,
                                     leftDataframe: String,
                                     rightDataframe: String,
                                     joinCondition: List[SingleJoinCondition],
                                     selectColumns: List[JoinSelectColumn])
  extends TransformationOptions(transformationType)

object JoinTransformationOptions {

  implicit def decodeJson: DecodeJson[JoinTransformationOptions] = DecodeJson.derive[JoinTransformationOptions]
}
