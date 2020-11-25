package it.luca.pipeline.test

import java.io.{BufferedWriter, File, FileWriter}

import argonaut.Argonaut._
import argonaut._
import org.apache.log4j.Logger

import scala.reflect.runtime.universe._

abstract class AbstractJsonSpec extends AbstractSpec {

  private final val logger = Logger.getLogger(getClass)

  final def toJsonString[T](tObject: T)(implicit encodeJson: EncodeJson[T], typeTag: TypeTag[T]): String = {

    val tClassName: String = typeOf[T].typeSymbol.name.toString
    logger.info(s"Trying to parse provided object of type ($tClassName) as a json string")
    val jsonString: String = tObject.jencode.spaces4
    logger.info(s"Successfully parsed provided object of type ${className[T]} as a json string. Result: \n\n$jsonString\n")
    jsonString
  }

  final def writeJsonFile[T](tObject: T, fileName: String)(implicit encodeJson: EncodeJson[T], typeTag: TypeTag[T]): Unit = {

    val bufferedWriter = new BufferedWriter(new FileWriter(new File(fileName)))
    bufferedWriter.write(toJsonString(tObject))
    bufferedWriter.close()
    logger.info(s"Successfully created .json file '$fileName")
  }

  final def deleteFile(fileName: String): Unit = new File(fileName).delete()
}
