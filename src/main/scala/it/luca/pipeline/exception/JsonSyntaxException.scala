package it.luca.pipeline.exception

case class JsonSyntaxException(jsonString: String)
  extends Throwable(s"Provided .json string does not contain valid .json syntax. " +
    s"Check it yourself using Notepad ++ exploiting JSON Viewer plugin. Provided string:\n$jsonString\n")
