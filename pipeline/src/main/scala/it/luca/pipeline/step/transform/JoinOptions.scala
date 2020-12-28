package it.luca.pipeline.step.transform

import argonaut.DecodeJson

case class JoinTransformationOptions(override val transformationType: String,
                                     override val transformationOrder: Int,
                                     joinOptions: JoinOptions)
  extends TransformationOptions(transformationType, transformationOrder)

object JoinTransformationOptions {

  implicit def decodeJson: DecodeJson[JoinTransformationOptions] = DecodeJson.derive[JoinTransformationOptions]
}

case class JoinOptions(joinType: String, rightAlias: String, joinCondition: List[SingleJoinCondition], selectColumns: List[JoinSelectColumn])

object JoinOptions {

  implicit def decodeJson: DecodeJson[JoinOptions] = DecodeJson.derive[JoinOptions]
}

case class SingleJoinCondition(leftSide: String, rightSide: String) {

  override def toString: String = s"($leftSide.equalTo($rightSide)"
}

object SingleJoinCondition {

  implicit def decodeJson: DecodeJson[SingleJoinCondition] = DecodeJson.derive[SingleJoinCondition]
}

case class JoinSelectColumn(side: String, expression: String, alias: Option[String])

object JoinSelectColumn {

  implicit def decodeJson: DecodeJson[JoinSelectColumn] = DecodeJson.derive[JoinSelectColumn]
}
