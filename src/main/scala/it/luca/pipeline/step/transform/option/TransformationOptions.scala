package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}

abstract class TransformationOptions(val transformationType: String)

object TransformationOptions extends DecodeJsonSubTypes[TransformationOptions] {

  implicit def decodeJson: DecodeJson[TransformationOptions] = decodeSubTypes(JsonField.TransformationType.label,
    JsonValue.WithColumnTransformation.value -> WithColumnTransformationOptions.decodeJson,
    JsonValue.DropColumnTransformation.value -> DropColumnTransformationOptions.decodeJson,
    JsonValue.SelectTransformation.value -> SelectTransformationOptions.decodeJson)
}