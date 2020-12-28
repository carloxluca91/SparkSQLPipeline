package it.luca.pipeline.step.transform

import argonaut.DecodeJson

case class FilterOptions(override val transformationType: String, override val transformationOrder: Int, filterCondition: String)
  extends TransformationOptions(transformationType, transformationOrder)

object FilterOptions {

  implicit def decodeJson: DecodeJson[FilterOptions] = DecodeJson.derive[FilterOptions]
}
