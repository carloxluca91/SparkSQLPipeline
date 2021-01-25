package it.luca.pipeline.json

import argonaut._

trait DecodeJsonSubTypes[T] {

  protected val discriminatorField: String
  protected val subclassesEncoders: Map[String, DecodeJson[_ <: T]]

  private def getDecoderForValue(h: CursorHistory, value: String): DecodeResult[DecodeJson[_ <: T]] = {

    subclassesEncoders
      .get(value)
      .fold(DecodeResult.fail[DecodeJson[_ <: T]](s"Unknown value: $value", h))(d => DecodeResult.ok(d))
  }
  implicit def decodeJson: DecodeJson[T] = {

    DecodeJson[T] { c: HCursor =>

      val discriminatingValueCursor: ACursor = c.downField(discriminatorField)
      for {

        discriminatingValue <- discriminatingValueCursor.as[String]
        decoder  <- getDecoderForValue(discriminatingValueCursor.history, discriminatingValue)
        data <- decoder(c).map[T](identity)

      } yield data
    }
  }
}
