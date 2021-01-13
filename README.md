# SparkSQLPipeline

Scala project for automatic execution of ETL processes expressed 
as `.json` files (denoted as `pipelines`) by means of `Spark` API

As each `Spark` application usually consists of some 
`read`, `transform` and `write` operations, my goal was 
to develop a project able to execute almost any combination 
of such basic tasks by parsing a suitably written `.json` file

The project consists of two sub-modules

* `sparkSQL` which provides a hand-made SQL catalog as well as some extensions of 
standard `Spark` functionalities

* `pipeline` which contains models and logic representing a pipeline

Some details:

* Spark version: 2.4.0

* Scala version: 2.11.12

* Argonaut (for `.json` parsing) version: 6.2.2 




