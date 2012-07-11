# Riak tools
A set of command line tools for Riak, icluding:

- Copy - a tool for copying data between two clusters (use com.riak.tools.Copy to run)
- Mapred - a tool for running mapreduce jobs (com.riak.tools.Mapred to run)

## Prerequisites

* sbt 0.11.2
* Scala 2.9.1

## Usage

* Build: `sbt package-dist` to build a package
* Run: `java -cp dist/riak-tools-1.0/riak-tools_2.9.1-1.0.jar com.riak.tools.Copy`