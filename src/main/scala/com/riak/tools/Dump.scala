package com.riak.tools

import java.io._
import scala.io._
import com.basho.riak.client.IRiakObject
import com.riak.client.RiakClient
import com.typesafe.config.ConfigFactory

import akka.actor.actorRef2Scala
import akka.actor.{Props, ActorSystem, Actor}
import akka.routing.{RoundRobinRouter, Broadcast}
import scopt.immutable.OptionParser
import com.riak.utils.Keys

object Dump {
	
	case class Config(
			source: String = "conf/source.nodes",
			bucket: String = "",
			output: String = "dump.out",
			keyPrefixes: String = "",
			keysAlphabet: String = "0123456789abcdefghjkmnpqrstvwxyz", //Crockford Base32
			keysAlphabetEnding: String = "~",
			retries: Int = 3,
			sep: String = "\n")
			
	def main(args: Array[String]) {
		val parser = new OptionParser[Config]("Dump", "1.0 - a tool for dumping keys to a local file") { def options = Seq(
    		opt("s", "source", "<file>", "source nodes ('host:httpport') list filename. Default: %s".format(Config().source)) 
    			{(v: String, c: Config) => c.copy(source = v)},
    		opt("o", "out", "<file>", "output filename. Default: %s".format(Config().output)) 
    			{(v: String, c: Config) => c.copy(output = v)},
    		opt("kr", "keyPrefixes", "<file>", "a file containing key prefixes")
    			{(v: String, c: Config) => c.copy(keyPrefixes = v)},
    		opt("k", "keysAlphabet", "s", "source keys alphabet. Default: %s".format(Config().keysAlphabet)) 
    			{(v: String, c: Config) => c.copy(keysAlphabet = v)},
    		opt("ke", "keysAlphabetEnding", "s", "source keys alphabet ending. Must be > last letter in alphabet. Default: %s".format(Config().keysAlphabetEnding)) 
    			{(v: String, c: Config) => c.copy(keysAlphabetEnding = v)},
    		intOpt("r", "numOfRetries", "number of retries is case of failure. Default: %s".format(Config().retries)) 
    			{(v: Int, c: Config) => c.copy(retries = v)},
    		opt("s", "separator", "s", "output file separator. Default: \\n")
    			{(v: String, c: Config) => c.copy(sep = v)},    		
    		arg("bucket", "a bucket name to copy") {(v: String, c: Config) => c.copy(bucket = v)}
    	)}
    	parser.parse(args, Config()) map { dump(_) }
	}
	
	def dump(config: Dump.Config) {
		val sc = RiakClient.newInstance(config.source)
		try {
			val out = new PrintWriter(new File(config.output))

			val keyRanges = config.keyPrefixes match {
			  case kp: String if kp.length > 0 => Keys.generateKeyRangesFromFile(config.keyPrefixes, config.keysAlphabetEnding)
			  case _ => Keys.generateKeyRanges(config.keysAlphabet, config.keysAlphabetEnding)
			}
			val totalRanges = keyRanges.length
			var (curr, totalKeys) = (1, 0)
			keyRanges foreach { range => {
				print("Processing key range [%s, %s] (%d out of %d) ...".format(range._1, range._2, curr, totalRanges))
				val count = dumpRange(sc, out, config, range._1, range._2)
				println(" Dumped %d keys.".format(count))
				totalKeys += count; curr += 1
			}}
			println("Done. Dumped %d keys in %d ranges".format(totalKeys, totalRanges))
			out.close()
		}
		finally { sc.shutdown() }		
	}
	def dumpRange(sc: RiakClient, out: PrintWriter, config: Config, from: String, to: String): Int = {
	  var count = 0
	  var success = false
	  (0 until config.retries).toStream.takeWhile(_ => !success).foreach { _ =>
	    try {
			sc.keysRange(config.bucket, from, to) foreach { k => out.write(k + config.sep); count += 1 }
			success = true
		}
		catch { case e => { println("dumpRange: Error occured - " + e.getMessage); e.printStackTrace } }
	  }
	  count
	}

}