package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.common.MultipleSrcTransformationOptions

case class JoinTransformationOptions(override val transformationType: String,
                                     joinOptions: JoinOptions)
  extends MultipleSrcTransformationOptions(transformationType)

object JoinTransformationOptions {

  implicit def decodeJson: DecodeJson[JoinTransformationOptions] = DecodeJson.derive[JoinTransformationOptions]
}

case class JoinOptions(joinType: String,
                       leftDataframe: String,
                       rightDataframe: String,
                       joinCondition: List[SingleJoinCondition],
                       selectColumns: List[JoinSelectColumn])

object JoinOptions {

  implicit def decodeJson: DecodeJson[JoinOptions] = DecodeJson.derive[JoinOptions]
}

case class SingleJoinCondition(leftSide: String,
                               operator: String,
                               rightSide: String) {

  override def toString: String = s"($leftSide.$operator($rightSide)"
}

object SingleJoinCondition {

  implicit def decodeJson: DecodeJson[SingleJoinCondition] = DecodeJson.derive[SingleJoinCondition]
}

case class JoinSelectColumn(side: String,
                            expression: String,
                            alias: Option[String])

object JoinSelectColumn {

  implicit def decodeJson: DecodeJson[JoinSelectColumn] = DecodeJson.derive[JoinSelectColumn]
}
