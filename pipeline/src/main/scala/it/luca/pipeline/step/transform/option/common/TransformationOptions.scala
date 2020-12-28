package it.luca.pipeline.step.transform.option.common

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}
import it.luca.pipeline.step.transform.option.concrete._

abstract class TransformationOptions(val transformationType: String)

object TransformationOptions extends DecodeJsonSubTypes[TransformationOptions] {

  override protected val discriminatorField: String = JsonField.TransformationType.label
  override protected val subclassesEncoders: Map[String, DecodeJson[_ <: TransformationOptions]] = Map(
    JsonValue.WithColumn -> WithColumnTransformationOptions.decodeJson,
    JsonValue.Drop -> DropColumnTransformationOptions.decodeJson,
    JsonValue.Select -> SelectTransformationOptions.decodeJson,
    JsonValue.WithColumnRenamed -> WithColumnRenamedTransformationOptions.decodeJson,
    JsonValue.Join -> JoinTransformationOptions.decodeJson,
    JsonValue.Union -> UnionTransformationOptions.decodeJson)
}