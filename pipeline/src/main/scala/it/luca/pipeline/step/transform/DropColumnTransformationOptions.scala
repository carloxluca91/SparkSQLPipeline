package it.luca.pipeline.step.transform

import argonaut.DecodeJson

case class DropColumnTransformationOptions(override val transformationType: String, override val transformationOrder: Int, columns: List[String])
  extends TransformationOptions(transformationType, transformationOrder)

object DropColumnTransformationOptions {

  implicit def decodeJson: DecodeJson[DropColumnTransformationOptions] = DecodeJson.derive[DropColumnTransformationOptions]
}
