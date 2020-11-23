package it.luca.pipeline.json

import argonaut._

trait DecodeJsonSubTypes[T] {

  protected def decodeSubTypes(discriminatingFieldLabel: String, encoders: (String, DecodeJson[_ <: T]) *): DecodeJson[T] = {

    val encodersMap: Map[String, DecodeJson[_ <: T]] = encoders.toMap

    def getDecoderForValue(h: CursorHistory, value: String): DecodeResult[DecodeJson[_ <: T]] = {

      encodersMap
        .get(value)
        .fold(DecodeResult.fail[DecodeJson[_ <: T]](s"Unknown value: $value", h))(d => DecodeResult.ok(d))
    }

    DecodeJson[T] { c: HCursor =>

      val discriminatingValueCursor: ACursor = c.downField(discriminatingFieldLabel)
      for {

        discriminatingValue <- discriminatingValueCursor.as[String]
        decoder  <- getDecoderForValue(discriminatingValueCursor.history, discriminatingValue)
        data <- decoder(c).map[T](identity)

      } yield data
    }
  }
}
