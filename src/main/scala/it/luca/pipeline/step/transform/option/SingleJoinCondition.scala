package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson

case class SingleJoinCondition(leftSide: String,
                               operator: String,
                               rightSide: String) {

  override def toString: String = s"($leftSide.$operator($rightSide)"
}

object SingleJoinCondition {

  implicit def decodeJson: DecodeJson[SingleJoinCondition] = DecodeJson.derive[SingleJoinCondition]
}
