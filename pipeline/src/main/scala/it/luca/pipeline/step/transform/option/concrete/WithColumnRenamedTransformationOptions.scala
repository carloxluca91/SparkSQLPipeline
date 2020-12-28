package it.luca.pipeline.step.transform.option.concrete

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.option.common.SingleSrcTransformationOptions

case class WithColumnRenamedTransformationOptions(override val transformationType: String,
                                                  override val inputDfId: String,
                                                  columns: List[WithColumnRenamedOption])
  extends SingleSrcTransformationOptions(transformationType, inputDfId)

object WithColumnRenamedTransformationOptions {

  implicit def decodeJson: DecodeJson[WithColumnRenamedTransformationOptions] = DecodeJson.derive[WithColumnRenamedTransformationOptions]
}

case class WithColumnRenamedOption(oldName: String, newName: String)

object WithColumnRenamedOption {

  implicit def decodeJson: DecodeJson[WithColumnRenamedOption] = DecodeJson.derive[WithColumnRenamedOption]
}