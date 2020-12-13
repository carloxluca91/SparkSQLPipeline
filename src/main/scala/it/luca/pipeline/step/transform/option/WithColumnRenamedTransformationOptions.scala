package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.common.SingleSrcTransformationOptions

case class WithColumnRenamedTransformationOptions(override val transformationType: String,
                                                  override val inputSourceId: String,
                                                  columns: List[WithColumnRenamedOption])
  extends SingleSrcTransformationOptions(transformationType, inputSourceId)

object WithColumnRenamedTransformationOptions {

  implicit def decodeJson: DecodeJson[WithColumnRenamedTransformationOptions] = DecodeJson.derive[WithColumnRenamedTransformationOptions]
}

case class WithColumnRenamedOption(oldName: String, newName: String)

object WithColumnRenamedOption {

  implicit def decodeJson: DecodeJson[WithColumnRenamedOption] = DecodeJson.derive[WithColumnRenamedOption]
}