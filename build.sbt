// Dependencies versions
val sparkVersion = "2.4.0"
val argonautVersion = "6.2.2"
val scoptVersion = "3.5.0"
val scalaTestVersion = "3.2.0"

// Common settings
lazy val commonSettings = Seq(

  scalaVersion := "2.11.12",
  organization := "it.luca",
  version := "0.1.0",

  // Scala compiler options
  scalacOptions ++= Seq(

    "-encoding", "UTF-8",
    "-target:jvm-1.8"
  ),

  // Exclude .properties file from packaging
  (unmanagedResources in Compile) := (unmanagedResources in Compile).value
    .filterNot(_.getName.endsWith(".properties")),

  // Jar name and merging strategy
  assemblyJarName in assembly := s"${name.value}_${version.value}.jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }

)

lazy val root = (project in file("."))
  .aggregate(sparkSQL, pipeline)
  .settings(

    commonSettings,
    name := "sparkSQLPipeline"
  )

lazy val sparkSQL = (project in file("sparkSQL"))
  .settings(

    commonSettings,
    name := "sparkSQL",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.scalactic" %% "scalactic" % scalaTestVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    )
  )

lazy val pipeline = (project in file("pipeline"))
  .settings(

    commonSettings,
    name := "pipeline",
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % scoptVersion,
      "io.argonaut" %% "argonaut" % argonautVersion,
      "io.argonaut" %% "argonaut-scalaz" % argonautVersion,
      "io.argonaut" %% "argonaut-monocle" % argonautVersion,
      "io.argonaut" %% "argonaut-cats" % argonautVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.scalactic" %% "scalactic" % scalaTestVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    )
  ).dependsOn(sparkSQL)
