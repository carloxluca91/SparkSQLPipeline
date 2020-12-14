val argonautVersion = "6.2.2"
val sparkVersion = "2.2.3"
val scoptVersion = "3.5.0"
val scalaTestVersion = "3.2.0"
val sparkSQLibraryVersion = "0.0.3"

lazy val pipelineRunnerScala = (project in file("."))
  .settings(

    name := "pipelineRunnerScala",
    version := "0.1.0",
    scalaVersion := "2.11.8",
    scalacOptions ++= Seq(

      "-encoding", "UTF-8",
      "-target:jvm-1.8"
    ),

    libraryDependencies ++= Seq(

      "com.github.scopt" %% "scopt" % scoptVersion,
      "io.argonaut" %% "argonaut" % argonautVersion,
      "io.argonaut" %% "argonaut-scalaz" % argonautVersion,
      "io.argonaut" %% "argonaut-monocle" % argonautVersion,
      "io.argonaut" %% "argonaut-cats" % argonautVersion,
      "it.luca.spark" %% "sparkSQL" % sparkSQLibraryVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.scalactic" %% "scalactic" % scalaTestVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ),

    assemblyJarName in assembly := s"${name.value}_${version.value}.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )