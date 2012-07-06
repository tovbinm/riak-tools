
package com.riak.tools

import java.io._
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.io._
import akka.actor.ActorSystem
import akka.actor.{Props, ActorRef, Actor}
import akka.routing.RoundRobinRouter
import scopt.immutable.OptionParser
import com.riak.client.RiakClient
import com.basho.riak.client.raw.pbc.PBClusterClient
import com.basho.riak.client.raw.query.indexes.BinRangeQuery
import com.basho.riak.client.query.indexes.KeyIndex
import akka.routing.Broadcast


object Copy {	
	case class Config(
			source: String = "conf/source.nodes",
			keysAlphabet: String = "0123456789abcdefghjkmnpqrstvwxyz", //Crockford Base32
			destination: String = "conf/destination.nodes", 
			sourceBucket: String = "",
			destBucket: String = "",
			stopOnFetchConflicts: Boolean = false,
			timeoutMs: Long = 3600*1000,
			numOfWorkers: Int = 10,
			printProgressEvery: Int = 1000)
	
    def main(args: Array[String]) {
    	val parser = new OptionParser[Config]("Copy", "1.0 - a tool for copying data between Riak clusters") { def options = Seq(
    		opt("s", "source", "<file>", "source nodes list filename. Default: %s".format(Config().source)) 
    			{(v: String, c: Config) => c.copy(source = v)},
    		opt("k", "keysAlphabet", "s", "source keys alphabet. Default: %s".format(Config().keysAlphabet)) 
    			{(v: String, c: Config) => c.copy(keysAlphabet = v)},
    		opt("d", "dest", "<file>", "destination nodes list filename. Default: %s".format(Config().destination)) 
    			{(v: String, c: Config) => c.copy(destination = v)},
    		intOpt("t", "timeout", "n", "timeout in ms for Riak operations. Default: %s ms".format(Config().timeoutMs)) 
    			{(v: Int, c: Config) => c.copy(timeoutMs = v.toLong)},
    		intOpt("w", "numOfWorkers", "n", "number of workers. Default: %s".format(Config().numOfWorkers)) 
    			{(v: Int, c: Config) => c.copy(numOfWorkers = v)},
    		intOpt("p", "printProgressEvery", "n", "print progress rate. Default: %s items".format(Config().printProgressEvery)) 
    			{(v: Int, c: Config) => c.copy(printProgressEvery = v)},
    		flag("sfc", "stopOnFetchConflicts", "stop on fetch conflicts.") {_.copy(stopOnFetchConflicts = true)},
    		arg("sourceBucket", "source bucket name") {(v: String, c: Config) => c.copy(sourceBucket = v)},
    		arg("destBucket", "destination bucket name") {(v: String, c: Config) => c.copy(destBucket = v)}
    	)}
    	parser.parse(args, Config()) map { copy(_) }
    }
	
	var sc: RiakClient = null
	var dc: RiakClient = null
	var conf: Config = null
	
	def copy(config: Copy.Config) {
		conf = config
		sc = RiakClient.newInstance(conf.source)
		dc = RiakClient.newInstance(conf.destination)
		val copySystem = ActorSystem("copySystem")
		val copyMaster = copySystem.actorOf(Props(new CopyMaster()), name = "copyMaster")
		copyMaster ! Copy
		copySystem.registerOnTermination({ sc.shutdown(); dc.shutdown() })
	}
}

case class Copy
case class NextKeyRange(from: String, to: String)
case class EndOfKeyRange
case class EndOfCopy


class CopyMaster() extends Actor {
	val conf = Copy.conf
	val workerRouter = context.actorOf(Props[CopyWorker].withRouter(RoundRobinRouter(conf.numOfWorkers)), name = "workerRouter")
	val keyRanges: Seq[(String,String)] = generateKeyRanges(conf.keysAlphabet)	
	var (count, keyRangeInd) = (0, 0)
	var lastTs = System.currentTimeMillis
	var workersDone = 0

	
	def receive = {		
		case Copy => self ! nextKeyRange()		
		case c: NextKeyRange => {
			val keys = Copy.sc.keysRange(conf.sourceBucket, c.from, c.to)
			println("Copying range [%s, %s]. Total %d items".format(c.from, c.to, keys.length))			
			keys.foreach { workerRouter ! _ }
			workerRouter ! Broadcast(EndOfKeyRange())
		}
		case c: Int => {
			count += c
			if (count % conf.printProgressEvery == 0) {
				val now = System.currentTimeMillis				
				println("Copied %d items. Elapsed: %dms".format(count, now - lastTs))
				lastTs = now
			}
		}
		case EndOfKeyRange() => {
			workersDone = workersDone + 1
			if (workersDone == conf.numOfWorkers) {
				self ! nextKeyRange()
				workersDone = 0
			}
		}
		case EndOfCopy() => { context.stop(self); context.system.shutdown() }
	}
	
	def generateKeyRanges(keysAlphabet: String): Seq[(String,String)] = {
    	val k = keysAlphabet.toSeq.sorted
    	(if (k.length % 2 == 0) k ++ Seq(k.last, "~") else k ++ "~")
    	.grouped(2).collect{ case Seq(a,b) => (a.toString,b.toString) }.toSeq
    }
	
	def nextKeyRange(): Any = {
		if (keyRangeInd >= keyRanges.length) return EndOfCopy()

		val kr = keyRanges(keyRangeInd)
		keyRangeInd = keyRangeInd + 1
		NextKeyRange(kr._1, kr._2)
	}
}

class CopyWorker extends Actor {
	val conf = Copy.conf
	def receive = {		
		case key: String =>{
			Copy.sc.get(conf.sourceBucket, key, conf.stopOnFetchConflicts) match {    		
    			case value: String => { Copy.dc.set(conf.destBucket, key, value); sender ! 1 }
    			case _ => println("No value for key '%s'".format(key))
			}			
		}
		case EndOfKeyRange() => sender ! EndOfKeyRange()
	}	
}

