package it.luca.pipeline.option

import scopt.OptionParser

object ScoptParser {

  case class Config(pipelineName: String = "", jobPropertiesFile: String = "") {

    override def toString: String = {

      s"Input configuration:\n\n" +
        s"    -${ScoptOption.PipelineName.shortOption} (--${ScoptOption.PipelineName.longOption}) = '$pipelineName'," +
        s"    -${ScoptOption.PropertiesFile.shortOption} (--${ScoptOption.PropertiesFile.description}) = '$jobPropertiesFile'"
    }
  }

  val configParser: OptionParser[Config] = new OptionParser[Config]("scopt 3.5.0") {

   opt[String](ScoptOption.PipelineName.shortOption, ScoptOption.PipelineName.longOption)
     .text(ScoptOption.PipelineName.description)
     .required()
     .action((x, c) => c.copy(pipelineName = x))

    opt[String](ScoptOption.PropertiesFile.shortOption, ScoptOption.PropertiesFile.longOption)
      .text(ScoptOption.PropertiesFile.description)
      .required()
      .action((x, c) => c.copy(jobPropertiesFile = x))
  }
}
