# PipelineRunnerScala

Scala project for automatic execution of multi-step ETL processes 
expressed as `JSON` files (denoted as `pipelines`) by means of `Spark SQL` API

As each `Spark` application usually consists of some *Read*, *Transform* and *Write* operations, 
my goal was to develop a project that could allow executing as many as possible 
ETL processes that combines such basic operations

Some (brief) details:

* Spark version: 2.2.3

* Scala version: 2.11.8

* Argonaut (for JSON parsing) version: 6.2.2 

* SBT version 1.3.8



