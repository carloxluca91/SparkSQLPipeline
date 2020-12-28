package it.luca.pipeline.step.transform

import argonaut.DecodeJson

case class UnionOptions(override val transformationType: String, override val transformationOrder: Int, unionAliases: List[String])
  extends TransformationOptions(transformationType, transformationOrder)

object UnionOptions {

  implicit def decodeJson: DecodeJson[UnionOptions] = DecodeJson.derive[UnionOptions]
}
