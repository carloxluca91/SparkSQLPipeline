package it.luca.pipeline.step.common

import argonaut._
import it.luca.pipeline.json.{DecodeJsonIntoSubTypes, JsonField}
import it.luca.pipeline.step.read.ReadStep
import it.luca.pipeline.step.transform.TransformStep
import it.luca.pipeline.step.write.WriteStep

abstract class AbstractStep(val name: String, val description: String, val stepType: String, val dataframeId: String)

object AbstractStep extends DecodeJsonIntoSubTypes[AbstractStep]{

  implicit def AbstractStepDecodeJson: DecodeJson[AbstractStep] = decodeSubTypes(JsonField.StepType.label,
    "read" -> DecodeJson.derive[ReadStep],
    "transform" -> DecodeJson.derive[TransformStep],
    "write" -> DecodeJson.derive[WriteStep])
}
