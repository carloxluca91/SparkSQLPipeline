# SparkSQLPipeline

Scala project for automatic execution of ETL processes expressed 
as *JSON* files (denoted as `pipelines`) by means of `Spark SQL` API

As each `Spark` application usually consists of some 
*Read*, *Transform* and *Write* operations, my goal was 
to develop a project that could allow executing as many as possible 
ETL processes that combines such basic operations

The project consists of two sub-modules

* `sparkSQL` which provides a hand-made SQL catalog as well as some extensions of 
standard Spark functionalities

* `pipeline` which contains models and logic representing a pipeline

Some details:

* Spark version: 2.4.0

* Scala version: 2.11.12

* Argonaut (for *JSON* parsing) version: 6.2.2 




