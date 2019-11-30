name := "disruptive-innovation"

organization := "com.monovore"

libraryDependencies += "org.typelevel" %% "cats-core" % "2.0.0"
libraryDependencies += "org.typelevel" %% "cats-free" % "2.0.0"
libraryDependencies += "com.lmax" % "disruptor" % "3.4.2"

scalaVersion := "2.12.8"

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full)
