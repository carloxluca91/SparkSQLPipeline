package it.luca.pipeline.json

object JsonField extends Enumeration {

  protected case class Val(label: String) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToJsonFieldVal(x: Value): Val = x.asInstanceOf[Val]

  val DataframeId: Val = Val("dataframeId")
  val Description: Val = Val("description")
  val Header: Val = Val("header")
  val Name: Val = Val("name")
  val Path: Val = Val("path")
  val PipelineSteps: Val = Val("pipelineSteps")
  val SchemaFile: Val = Val("schemaFile")
  val Separator: Val = Val("separator")
  val SourceType: Val = Val("sourceType")
  val SrcOptions: Val = Val("srcOptions")
  val StepType: Val = Val("stepType")

}
