# Riak command line tools
A set of command line tools for Riak, including:

- Copy - copy specified bucket contents between two clusters
- MapRed - run mapreduce jobs and write results to a local file
- Dump - dump all bucket keys to a local file 

## Prerequisites

* sbt 0.11.3
* scala 2.9.1

## Usage

* Build: `sbt package-dist` to build a package
* Run: `java -cp dist/riak-tools-1.0/riak-tools_2.9.1-1.0.jar com.riak.tools.Copy`