package com.riak.tools

import java.io._
import java.util.HashMap
import scala.io._
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import com.basho.riak.client.IRiakObject
import com.riak.client.RiakClient
import com.typesafe.config.ConfigFactory

import akka.actor.actorRef2Scala
import akka.actor.{Props, ActorSystem, Actor}
import akka.routing.{RoundRobinRouter, Broadcast}
import scopt.immutable.OptionParser

object MapRed {
	
	case class Config(
			source: String = "conf/source.nodes",
			bucket: String = "",
			mapJS: String = "conf/map.js",
			reduceJS: String ="conf/reduce.js",
			arg: String = "",
			argMap: HashMap[String, String] = null,
			timeoutMs: Long = 10*60*1000,
			output:String = "mapred.out",
			sep: String = "\001")
			
	def main(args: Array[String]) {
		val parser = new OptionParser[Config]("Mapred", "1.0 - a tool for running mapreduce jobs") { def options = Seq(
			opt("s", "source", "<file>", "source nodes list filename. Default: %s".format(Config().source)) 
    			{(v: String, c: Config) => c.copy(source = v)},
    		opt("m", "mapjs", "<file>", "map JS function filename. Default: %s".format(Config().mapJS)) 
    			{(v: String, c: Config) => c.copy(mapJS = v)},
    		opt("m", "redjs", "<file>", "reduce JS function filename. Default: %s".format(Config().reduceJS)) 
    			{(v: String, c: Config) => c.copy(reduceJS = v)},
    		opt("a", "arg", "s", "list of arguments for mapreduce job. Format: arg1=v1,arg2=v2,arg3=v3".format(Config().arg)) 
    			{(v: String, c: Config) => c.copy(arg = v)},
    		opt("o", "out", "<file>", "output filename. Default: %s".format(Config().output)) 
    			{(v: String, c: Config) => c.copy(output = v)},
    		opt("s", "separator", "s", "output file separator. Default: Ctrl+A") 
    			{(v: String, c: Config) => c.copy(sep = v)},
    		intOpt("t", "timeout", "n", "timeout in ms for Riak mapreduce. Default: %s ms".format(Config().timeoutMs)) 
    			{(v: Int, c: Config) => c.copy(timeoutMs = v.toLong)},    		
    		arg("bucket", "a bucket name to run mapreduce") {(v: String, c: Config) => c.copy(bucket = v)}
    	)}
    	parser.parse(args, Config()) map { config => mapred(config.copy(argMap = parseMapredArg(config.arg))) }
	}
	
	
	def mapred(config: MapRed.Config) {
		var rc : RiakClient = null
		try {
			rc = RiakClient.newInstance(config.source)			
			
			val (mapJSfun, reduceJSfun) = (fromFile(config.mapJS), fromFile(config.reduceJS))
			
			println("Executing the following mapreduce job:")
			println("Bucket: " + config.bucket)
			println("Arg: " + config.argMap)
			println("Map: " + mapJSfun)
			println("Reduce: " + reduceJSfun)
			println("Out: " + config.output)
			
			val start = System.currentTimeMillis
			val res = rc.mapReduce(
						bucket = config.bucket, 
						mapFunctionJS = mapJSfun , 
						reduceFunctionJS = reduceJSfun, 
						arg = config.argMap, 
						timeoutMs = config.timeoutMs)
			
			val out = new PrintWriter(new File(config.output))
			var count = 0
			res.getResult[String](classOf[String]).asScala.foreach { r => out.write(r + config.sep); count = count + 1 }
			out.close()
			
			println("Job completed in %d seconds and produced %d items.\nBye!".format((System.currentTimeMillis - start) / 1000, count))
		
		
		} finally { if (rc!=null) rc.shutdown() }
	}
	
	private def parseMapredArg(arg: String): HashMap[String, String] = {		
		val h = new HashMap[String, String]()
		arg.split(",") map { _.split("=") } collect { case Array(n,v) => h.put(n,v) }
		h
	}
	
	private def fromFile(fileName: String) =
		Source.fromFile(fileName).mkString match { case s:String if s.length > 0 => s case _ => null }			

}