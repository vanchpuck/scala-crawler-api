name := "scala-crawler-api"

version := "0.1"

scalaVersion := "2.13.10"

// https://mvnrepository.com/artifact/com.google.guava/guava
libraryDependencies += "com.google.guava" % "guava" % "31.1-jre"
// https://mvnrepository.com/artifact/org.jsoup/jsoup
libraryDependencies += "org.jsoup" % "jsoup" % "1.15.3"
// https://mvnrepository.com/artifact/com.github.crawler-commons/crawler-commons
libraryDependencies += "com.github.crawler-commons" % "crawler-commons" % "1.4"
// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test
