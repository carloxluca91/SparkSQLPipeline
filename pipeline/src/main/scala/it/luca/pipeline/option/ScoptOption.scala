package it.luca.pipeline.option

object ScoptOption extends Enumeration {

  protected case class Val(shortOption: Char, longOption: String, description: String)
    extends super.Val

  implicit def toVal(x: Value): Val = x.asInstanceOf[Val]

  val PipelineName: Val = Val('n', "name", "Name of pipeline to be triggered")
  val PropertiesFile: Val = Val('p', "properties", "Path of .properties file")
}