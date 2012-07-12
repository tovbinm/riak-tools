# Riak command line tools
A set of command line tools for Riak, including:

- Copy - a tool for copying data between two clusters
- Mapred - a tool for running mapreduce jobs

## Prerequisites

* sbt 0.11.2
* scala 2.9.1

## Usage

* Build: `sbt package-dist` to build a package
* Run: `java -cp dist/riak-tools-1.0/riak-tools_2.9.1-1.0.jar com.riak.tools.Copy`