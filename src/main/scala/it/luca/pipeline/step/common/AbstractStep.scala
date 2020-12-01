package it.luca.pipeline.step.common

import argonaut._
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}
import it.luca.pipeline.step.read.ReadStep
import it.luca.pipeline.step.transform.TransformStep
import it.luca.pipeline.step.write.WriteStep

abstract class AbstractStep(val name: String, val description: String, val stepType: String, val dataframeId: String)

object AbstractStep extends DecodeJsonSubTypes[AbstractStep]{

  implicit def decodeJson: DecodeJson[AbstractStep] = decodeSubTypes(JsonField.StepType.label,
      JsonValue.ReadStep.value -> ReadStep.decodeJson,
      JsonValue.TransformStep.value -> TransformStep.decodeJson,
      JsonValue.WriteStep.value -> WriteStep.decodeJson)
}
