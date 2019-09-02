name := "hdfs-file-modificator"

version := "0.1"

scalaVersion := "2.10.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.3"

// https://mvnrepository.com/artifact/com.databricks/spark-avro
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.2.1"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0-RC1" % Test
