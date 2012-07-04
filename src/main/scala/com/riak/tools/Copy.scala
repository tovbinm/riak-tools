
package com.riak.tools

import java.io._

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.io._

import scopt.immutable.OptionParser
import com.riak.client.RiakClient
import com.basho.riak.client.raw.pbc.PBClusterClient
import com.basho.riak.client.raw.query.indexes.BinRangeQuery
import com.basho.riak.client.query.indexes.KeyIndex


object Copy {	
	case class Config(
			source: String = "conf/source.nodes",
			sourceKeys: String = "",
			destination: String = "conf/destination.nodes", 
			sourceBucket: String = "",
			destBucket: String = "",
			stopOnFetchConflicts: Boolean = false,
			timeoutMs:Long = 3600*1000,
			printProgressEvery: Int = 1000)
	
    def main(args: Array[String]) {
    	val parser = new OptionParser[Config]("Copy", "1.0 - a tool for copying data between Riak clusters") { def options = Seq(
    		opt("s", "source", "<file>", "source nodes list filename. Default: %s".format(Config().source)) 
    			{(v: String, c: Config) => c.copy(source = v)},
    		opt("k", "keys", "<file>", "source keys filename.") 
    			{(v: String, c: Config) => c.copy(sourceKeys = v)},
    		opt("d", "dest", "<file>", "destination nodes list filename. Default: %s".format(Config().destination)) 
    			{(v: String, c: Config) => c.copy(destination = v)},
    		intOpt("t", "timeout", "n", "timeout in ms for Riak operations. Default: %s ms".format(Config().timeoutMs)) 
    			{(v: Int, c: Config) => c.copy(timeoutMs = v.toLong)},
    		intOpt("p", "printProgressEvery", "n", "print progress rate. Default: %s items".format(Config().printProgressEvery)) 
    			{(v: Int, c: Config) => c.copy(printProgressEvery = v)},
    		flag("sfc", "stopOnFetchConflicts", "stop on fetch conflicts.") {_.copy(stopOnFetchConflicts = true)},
    		arg("sourceBucket", "source bucket name") {(v: String, c: Config) => c.copy(sourceBucket = v)},
    		arg("destBucket", "destination bucket name") {(v: String, c: Config) => c.copy(destBucket = v)}
    	)}
    	parser.parse(args, Config()) map { copy(_) } getOrElse { }
    }
	
    def copy(conf: Config) {

      print("Source: ")
      val sc = RiakClient.newInstance(conf.source)
      print("Destination: ")
      val dc = RiakClient.newInstance(conf.destination)
      
      try {
    	  val keys = sourceKeys(sc, conf)
    	  val total = keys.length
    	  var c = 0
    	  keys.foreach(k => { 
    	  	copyOne(sc, dc, conf, k)
    	  	c = c + 1 
    	  	if (c % conf.printProgressEvery == 0) println("Copied %d items, %d to go".format(c, total))    	  	
    	  })   	  
    	  println("%d items copied.\nDone.".format(c))
    	  
      } finally {
      	sc.shutdown; dc.shutdown
      }     
    }
    
    def copyOne(sc: RiakClient, dc: RiakClient, conf: Config, key: String) {
    	sc.get(conf.sourceBucket, key, conf.stopOnFetchConflicts) match {    		
    		case value: String => dc.set(conf.destBucket, key, value)
    		case _ => println("No value for key '%s'".format(key))
    	}
    }

    def sourceKeys(c: RiakClient, conf: Config): Seq[String] = {
    	conf.sourceKeys.length match {
    		case 0 => {
      			println ("No source keys file was specified. Going to run MapReduce on source bucket '%s'".format(conf.sourceBucket))
      			val mapFunc = "function(v, keydata, arg) { return [ v.key ]; }"
      			val res = c.mapReduce(conf.sourceBucket, mapFunc, timeoutMs = conf.timeoutMs)      			
      			val keys = res.getResult[String](classOf[String]).asScala.toSeq
      			writeKeys(keys)      			
      		}
      		case _ => {
      			println ("Using '%s' source keys file".format(conf.sourceKeys))
      			readKeys(conf.sourceKeys)
      		}
    	}    	
    }
    
    def writeKeys(keys: Seq[String]): Seq[String] = {
    	val w = new PrintWriter(new File("keys-" + System.currentTimeMillis))   	
    	keys.foreach(k => w.write( k + "\n" ))
    	w.close
    	keys
    }
    
    def readKeys(keysFile: String): Seq[String] = Source.fromFile(keysFile).getLines.toSeq   
    
}


