package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}

abstract class TransformationOptions(val transformationType: String, val transformationOrder: Int)

object TransformationOptions extends DecodeJsonSubTypes[TransformationOptions] {

  override protected val discriminatorField: String = JsonField.TransformationType.label
  override protected val subclassesEncoders: Map[String, DecodeJson[_ <: TransformationOptions]] = Map(
    JsonValue.Drop -> DropColumnTransformationOptions.decodeJson,
    JsonValue.Join -> JoinTransformationOptions.decodeJson,
    JsonValue.Select -> SelectOptions.decodeJson,
    JsonValue.Union -> UnionOptions.decodeJson,
    JsonValue.WithColumn -> WithColumnTransformationOptions.decodeJson,
    JsonValue.WithColumnRenamed -> WithColumnRenamedOptions.decodeJson)
}