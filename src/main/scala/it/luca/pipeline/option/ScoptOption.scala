package it.luca.pipeline.option

object ScoptOption extends Enumeration {

  protected case class Val(shortOption: Char, longOption: String, description: String)
    extends super.Val

  import scala.language.implicitConversions
  implicit def valueToScoptOptionVal(x: Value): Val = x.asInstanceOf[Val]

  val PipelineName: Val = Val('n', "name", "Name of pipeline ot be triggered")
  val PropertiesFile: Val = Val('p', "properties", "Path of .properties file")
}
