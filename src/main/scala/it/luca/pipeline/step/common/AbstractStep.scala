package it.luca.pipeline.step.common

import argonaut._
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}
import it.luca.pipeline.step.read.ReadStep
import it.luca.pipeline.step.transform.TransformStep
import it.luca.pipeline.step.write.WriteStep

abstract class AbstractStep(val name: String, val description: String, val stepType: String, val alias: String)

object AbstractStep extends DecodeJsonSubTypes[AbstractStep]{

  override protected val discriminatorField: String = JsonField.StepType
  override protected val subclassesEncoders: Map[String, DecodeJson[_ <: AbstractStep]] = Map(
    JsonValue.Read -> ReadStep.decodeJson,
    JsonValue.Transform -> TransformStep.decodeJson,
    JsonValue.Write -> WriteStep.decodeJson)
}
