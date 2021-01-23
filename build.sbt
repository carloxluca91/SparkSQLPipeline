// Dependencies versions
val hadoopCommonVersion = "3.0.0"
val sparkVersion = "2.4.0"
val argonautVersion = "6.2.2"
val scoptVersion = "3.5.0"
val scalaTestVersion = "3.2.0"

// Common settings
lazy val commonSettings = Seq(

  scalaVersion := "2.11.12",
  organization := "it.luca",
  version := "0.2.0",

  // Scala compiler options
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-target:jvm-1.8"
  ),

  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-common" % hadoopCommonVersion % "provided",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.scalactic" %% "scalactic" % scalaTestVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ),

  // Exclude .properties file from packaging
  (unmanagedResources in Compile) := (unmanagedResources in Compile).value
    .filterNot(_.getName.endsWith(".properties")),

  // Merging strategy
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

// aggregate project for running tasks on both subprojects (except assembly)
lazy val sparkSqlPipeline = (project in file("."))
  .aggregate(sparkSql, pipeline)

lazy val sparkSql = (project in file("sparkSql"))
  .settings(commonSettings: _*)

lazy val pipeline = (project in file("pipeline"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % scoptVersion,
      "io.argonaut" %% "argonaut" % argonautVersion,
      "io.argonaut" %% "argonaut-scalaz" % argonautVersion,
      "io.argonaut" %% "argonaut-monocle" % argonautVersion,
      "io.argonaut" %% "argonaut-cats" % argonautVersion
    ),

    // Jar name
    assemblyJarName in assembly := s"sparkSqlPipeline_${version.value}.jar",
  ).dependsOn(sparkSql)
