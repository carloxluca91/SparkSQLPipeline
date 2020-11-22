package it.luca.pipeline.json

import argonaut.DecodeJson

trait DecodeJsonDerive[T] {

  implicit def decodeJson: DecodeJson[T] = DecodeJson.derive[T]
}
