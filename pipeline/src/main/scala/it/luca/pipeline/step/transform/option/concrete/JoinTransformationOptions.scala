package it.luca.pipeline.step.transform.option.concrete

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.option.common.MultipleSrcTransformationOptions

case class JoinTransformationOptions(override val transformationType: String,
                                     override val inputDfIds: JoinInputSourceIds,
                                     joinOptions: JoinOptions)
  extends MultipleSrcTransformationOptions[JoinInputSourceIds](transformationType, inputDfIds)

object JoinTransformationOptions {

  implicit def decodeJson: DecodeJson[JoinTransformationOptions] = DecodeJson.derive[JoinTransformationOptions]
}

case class JoinInputSourceIds(left: String, right: String)

object JoinInputSourceIds {

  implicit def decodeJson: DecodeJson[JoinInputSourceIds] = DecodeJson.derive[JoinInputSourceIds]
}

case class JoinOptions(joinType: String,
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
