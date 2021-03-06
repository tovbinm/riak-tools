
package com.riak.tools

import com.basho.riak.client.IRiakObject
import com.riak.client.RiakClient
import com.typesafe.config.ConfigFactory

import akka.actor.actorRef2Scala
import akka.actor.{Props, ActorSystem, Actor}
import akka.routing.{RoundRobinRouter, Broadcast}
import scopt.immutable.OptionParser
import com.riak.utils.Keys


object Copy {	
	case class Config(
			source: String = "conf/source.nodes",
			keysAlphabet: String = "0123456789abcdefghjkmnpqrstvwxyz", //Crockford Base32
			keysAlphabetEnding: String = "~",
			keys: String = "",
			destination: String = "conf/destination.nodes",
			bucket: String = "",			
			stopOnFetchConflicts: Boolean = false,
			timeoutMs: Long = 3600*1000,
			numOfWorkers: Int = 10,
			printProgressEvery: Int = 1000)
	
    def main(args: Array[String]) {
    	val parser = new OptionParser[Config]("Copy", "1.0 - a tool for copying data between two clusters") { def options = Seq(
    		opt("s", "source", "<file>", "source nodes ('host:httpport') list filename. Default: %s".format(Config().source)) 
    			{(v: String, c: Config) => c.copy(source = v)},
    		opt("k", "keysAlphabet", "s", "source keys alphabet. Default: %s".format(Config().keysAlphabet)) 
    			{(v: String, c: Config) => c.copy(keysAlphabet = v)},
    		opt("ke", "keysAlphabetEnding", "s", "source keys alphabet ending. Must be > last letter in alphabet. Default: %s".format(Config().keysAlphabetEnding)) 
    			{(v: String, c: Config) => c.copy(keysAlphabetEnding = v)},
    		opt("ks", "keys", "<file>", "path to keys file '\\n' separated (may be used instead of keys alphabet)") 
    			{(v: String, c: Config) => c.copy(keys = v)},
    		opt("d", "dest", "<file>", "destination nodes ('host:httpport') list filename. Default: %s".format(Config().destination)) 
    			{(v: String, c: Config) => c.copy(destination = v)},
    		intOpt("t", "timeout", "n", "timeout in ms for Riak operations. Default: %s ms".format(Config().timeoutMs)) 
    			{(v: Int, c: Config) => c.copy(timeoutMs = v.toLong)},
    		intOpt("w", "numOfWorkers", "n", "number of workers. Default: %s".format(Config().numOfWorkers)) 
    			{(v: Int, c: Config) => c.copy(numOfWorkers = v)},
    		intOpt("p", "printProgressEvery", "n", "print progress rate. Default: %s items".format(Config().printProgressEvery)) 
    			{(v: Int, c: Config) => c.copy(printProgressEvery = v)},
    		flag("sfc", "stopOnFetchConflicts", "stop on fetch conflicts.") {_.copy(stopOnFetchConflicts = true)},
    		arg("bucket", "a bucket name to copy") {(v: String, c: Config) => c.copy(bucket = v)}
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
		val copySystem = ActorSystem("copy", ConfigFactory.load.getConfig("copy"))
		val copyMaster = copySystem.actorOf(Props(new CopyMaster()), name = "copyMaster")
		copyMaster ! Copy()
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
	val (isUsingKeysFile, keys) = conf.keys match {
		case k: String if k.length > 0 => {
			println("Copying using keys file '%s'".format(conf.keys))
			(true, Keys.getKeysFromFile(conf.keys))  
		}
		case _ => {
			println("Copying using keys alphabet '%s'".format(conf.keysAlphabet))
			(false, null)	
		}
	}
	val keyRanges = Keys.generateKeyRanges(conf.keysAlphabet, conf.keysAlphabetEnding)
	var (count, keyRangeInd) = (0, 0)
	var lastTs = System.currentTimeMillis
	var workersDone = 0
	
	def receive = {		
		case Copy() => {
			(isUsingKeysFile match {
				case true => {
					val batch = keys.take(conf.numOfWorkers * 1000).toSeq
					if (batch.size == 0) self ! EndOfCopy()
					else println("Copying next %d items".format(batch.size))
					batch
				}
				case _ => {
					val range = keyRanges.slice(keyRangeInd, keyRangeInd + 1)
					if (range.size == 0) { self ! EndOfCopy(); Seq() }
					else {
						val (from, to) = range.head
						keyRangeInd += 1
						val keys = Copy.sc.keysRange(conf.bucket, from, to)
						println("Copying range [%s, %s]. Total %d items".format(from, to, keys.length))
						keys
					}
				}
			}) foreach { workerRouter ! _ }
			workerRouter ! Broadcast(EndOfKeyRange())
		}
		case c: Int => {
			count += c
			if (count % conf.printProgressEvery == 0) {
				val now = System.currentTimeMillis				
				println("%d items processed. Elapsed: %dms".format(count, now - lastTs))
				lastTs = now
			}
		}
		case EndOfKeyRange() => {
			workersDone = workersDone + 1
			if (workersDone == conf.numOfWorkers) {
				workersDone = 0; self ! Copy()
			}
		}
		case EndOfCopy() => {
			println("Done. Total %d items processed.".format(count))
			context.stop(self); context.system.shutdown()
		}
	}
}

class CopyWorker extends Actor {
	val conf = Copy.conf
	def receive = {		
		case key: String => {
		  try {
			Copy.sc.get(conf.bucket, key, conf.stopOnFetchConflicts) match {
    			case item: IRiakObject => Copy.dc.set(item)
    			case _ => println("No value for key '%s'".format(key))
			}
		  } catch {
		    case e => {
		    	println("Failed to copy '%s'. Error: %s".format(key, e.getCause))
		  }}
		  finally { sender ! 1 }
		}
		case e: EndOfKeyRange => sender ! e
	}
}

