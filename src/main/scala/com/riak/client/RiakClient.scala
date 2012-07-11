package com.riak.client

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.Buffer
import scala.io.Source

import com.basho.riak.client.query.functions.JSSourceFunction
import com.basho.riak.client.query.indexes.KeyIndex
import com.basho.riak.client.query.{MapReduceResult, BucketMapReduce}
import com.basho.riak.client.raw.config.ClusterConfig
import com.basho.riak.client.raw.http.{HTTPClusterConfig, HTTPClusterClient}
import com.basho.riak.client.raw.query.indexes.BinRangeQuery
import com.basho.riak.client.raw.RiakResponse
import com.basho.riak.client.IRiakObject



class RiakClient (hostPortsFile: String, hostPorts: Seq[(String,Int)]) {

	def this(hostPortsFile: String) = this(hostPortsFile, RiakClient.getHostPorts(hostPortsFile))

	val rkv = {
      val clusterConf = new HTTPClusterConfig(ClusterConfig.UNLIMITED_CONNECTIONS)
      hostPorts.map { hp => new com.basho.riak.client.raw.http.HTTPClientConfig.Builder().withHost(hp._1).withPort(hp._2).build }
               .foreach { clusterConf.addClient(_) }
      new HTTPClusterClient(clusterConf)
	}
	
	override def toString() = hostPorts.mkString(",")
	
	def get(bucket: String, key: String, stopOnConflicts: Boolean): IRiakObject = {
	    rkv.fetch(bucket, key) match {
	    	case r: RiakResponse if r.hasValue && r.hasSiblings => {	    		
	    		stopOnConflicts match {
	    			case false => r.getRiakObjects.max(new Ordering[IRiakObject] {
	    							def compare(x: IRiakObject, y: IRiakObject): Int = x.getLastModified compareTo y.getLastModified
	    						 })
	    			case true => throw new Exception("Conflicts detected for '%s' in '%s'".format(key, bucket))
	    		}
	    	}
	    	case r: RiakResponse if r.hasValue => r.getRiakObjects()(0)
	    	case _ => null
	    }
	}
	
	def set(o: IRiakObject) = rkv.store(o)

	def del(bucket: String, key: String) = rkv.delete(bucket, key)
  
	def keysRange(bucket: String, from: String, to: String) : Buffer[String] = {
		val q = new BinRangeQuery(KeyIndex.index, bucket, from, to)
		rkv.fetchIndex(q).asScala
	}
	
	def mapReduce(bucket: String, mapFunctionJS: String = null, reduceFunctionJS: String = null, 
  		  		  arg: Object = null, timeoutMs: Long = 60000): MapReduceResult = {
    
		val mr = new BucketMapReduce(rkv, bucket)
    
		if (mapFunctionJS != null) mr.addMapPhase(new JSSourceFunction(mapFunctionJS), arg)
		if (reduceFunctionJS != null) mr.addReducePhase(new JSSourceFunction(reduceFunctionJS), arg)
    
		mr.timeout(timeoutMs).execute 
	}
	
	def shutdown() = rkv.shutdown()
	 
}


object RiakClient {

	private def getHostPorts(hostPortsFile: String): Seq[(String, Int)] = {
    	Source.fromFile(hostPortsFile).getLines.map { x => 
    		x.split(":") match {
    			case Array(host, port) => (host, port.toInt)
    			case _ => {}
    		}} collect { case x: (String, Int) => x } toSeq
    }
	
	def newInstance(hostPortsFile: String) = new RiakClient(hostPortsFile)

}