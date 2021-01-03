#!/bin/bash

log() {

  echo -e "$(date +"%Y-%m-%d %H:%M:%S")" "[$1]" "$2"
}

log "info" "Started bash script for Spark application submission"

pipelineNameOpt="-n"
pipelineName=""
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in

      # pipelineName
      "$pipelineNameOpt")
        pipelineName="$2"
        shift # past argument
        shift # past value
        ;;

      # unknown option
      *)
        log "warning" "Ignoring unrecognized option ($key) with value $2"
        shift # past argument
        shift # past value
        ;;
  esac
done

if [[ -z $pipelineName ]];
then
  log "error" "Pipeline name argument option ($pipelineNameOpt) not provided. Spark application will not be submitted"
else

  log "info" "Provided pipeline name: '$pipelineName'"
  applicationName="SparkSQLPipeline ($pipelineName) - App"
  applicationLibDir=hdfs:///user/osboxes/applications/spark_sql_pipeline/lib
  applicationJar="$applicationLibDir/sparkSQLPipeline_0.1.0.jar"
  applicationLog4j=spark_log4j.properties
  applicationProperties=spark_application.properties

  sparkApplicationHDFSLog4j="$applicationLibDir/$applicationLog4j"
  sparkApplicationHDFSProperties="$applicationLibDir/$applicationProperties"
  sparkSubmitFiles=$sparkApplicationHDFSLog4j,$sparkApplicationHDFSProperties

  mainClass=it.luca.pipeline.Main
  propertiesFileOpt="-p"
  mainClassArgs="$pipelineNameOpt $pipelineName $propertiesFileOpt $applicationProperties"

  log "info" "Proceeding with spark-submit command. Details:

        applicationName: $applicationName,
        spark submit files: $sparkSubmitFiles,
        main class: $mainClass,
        HDFS jar location: $applicationJar,
        application arguments: $mainClassArgs

      "

  spark-submit --master yarn --deploy-mode cluster \
    --name "$applicationName" \
    --files $sparkSubmitFiles \
    --driver-java-options "-Dlog4j.configuration=$applicationLog4j" \
    --class $mainClass \
    $applicationJar \
    $pipelineNameOpt "$pipelineName" $propertiesFileOpt $applicationProperties

  log "info" "Spark-submit completed"
fi