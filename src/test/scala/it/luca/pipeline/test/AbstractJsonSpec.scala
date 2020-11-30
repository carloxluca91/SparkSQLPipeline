package it.luca.pipeline.test

import java.io.{BufferedWriter, File, FileWriter}

import argonaut.Argonaut._
import argonaut._
import org.apache.log4j.Logger
import org.scalatest.BeforeAndAfterAll

import scala.reflect.runtime.universe._

trait AbstractJsonSpec extends AbstractSpec with BeforeAndAfterAll {

  private val logger = Logger.getLogger(getClass)
  private val testResourcesPath = "src/test/resources"
  protected val testJsonFilesToDelete: Seq[String]

  override protected def afterAll(): Unit = {

    if (testJsonFilesToDelete.nonEmpty) {

      val pathsToDelete = testJsonFilesToDelete.map(x => s"$testResourcesPath/$x")
      logger.info(s"Detected ${pathsToDelete.length} file to delete (${pathsToDelete.mkString(", ")})")
      pathsToDelete foreach { path => {
        new File(path).delete()
        logger.info(s"Successfully deleted .json file $path")
      }}
    }
  }

  final def toJsonString[T](tObject: T)(implicit encodeJson: EncodeJson[T], typeTag: TypeTag[T]): String = {

    val tClassName: String = typeOf[T].typeSymbol.name.toString
    logger.info(s"Trying to parse provided object of type $tClassName as a json string")
    val jsonString: String = tObject.jencode.spaces4
    logger.info(s"Successfully parsed provided object of type ${className[T]} as a json string. Result: \n\n$jsonString\n")
    jsonString
  }

  final def writeAsJsonFileInTestResources[T](tObject: T, fileName: String)(implicit encodeJson: EncodeJson[T], typeTag: TypeTag[T]): Unit = {

    val testResourceFilePath = s"src/test/resources/$fileName"
    val bufferedWriter = new BufferedWriter(new FileWriter(new File(testResourceFilePath)))
    bufferedWriter.write(toJsonString(tObject))
    bufferedWriter.close()
    logger.info(s"Successfully created .json file $testResourceFilePath")
  }

  final def asTestResource(fileName: String): String = s"$testResourcesPath/$fileName"
}
