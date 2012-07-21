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
			keysAlphabet: String = "0123456789abcdefghjkmnpqrstvwxyz", //Crockford Base32
			keysAlphabetEnding: String = "~",			
			sep: String = "\n")
			
	def main(args: Array[String]) {
		val parser = new OptionParser[Config]("Dump", "1.0 - a tool for dumping keys to a local file") { def options = Seq(
    		opt("s", "source", "<file>", "source nodes ('host:httpport') list filename. Default: %s".format(Config().source)) 
    			{(v: String, c: Config) => c.copy(source = v)},
    		opt("o", "out", "<file>", "output filename. Default: %s".format(Config().output)) 
    			{(v: String, c: Config) => c.copy(output = v)},
    		opt("k", "keysAlphabet", "s", "source keys alphabet. Default: %s".format(Config().keysAlphabet)) 
    			{(v: String, c: Config) => c.copy(keysAlphabet = v)},
    		opt("ke", "keysAlphabetEnding", "s", "source keys alphabet ending. Must be > last letter in alphabet. Default: %s".format(Config().keysAlphabetEnding)) 
    			{(v: String, c: Config) => c.copy(keysAlphabetEnding = v)},
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
			Keys.generateKeyRanges(config.keysAlphabet, config.keysAlphabetEnding) foreach { range => {
				print("Dumping keys range [%s, %s]...".format(range._1, range._2))
				var count = 0
				sc.keysRange(config.bucket, range._1, range._2) foreach { k => out.write(k + config.sep); count = count + 1 }
				println(" Dumped %d items.".format(count))
			}}
			println("Done")
			out.close()
		}
		finally { sc.shutdown() }
		
	}

}