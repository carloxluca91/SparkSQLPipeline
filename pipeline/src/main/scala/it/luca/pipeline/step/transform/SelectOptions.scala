package it.luca.pipeline.step.transform

import argonaut.DecodeJson

case class SelectOptions(override val transformationType: String, override val transformationOrder: Int, columns: List[String])
  extends TransformationOptions(transformationType, transformationOrder)

object SelectOptions {

  implicit def decodeJson: DecodeJson[SelectOptions] = DecodeJson.derive[SelectOptions]
}
