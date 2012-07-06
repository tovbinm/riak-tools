package com.riak.client

import scala.io.Source
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.Buffer
import com.basho.riak.client.builders.RiakObjectBuilder
import com.basho.riak.client.query.functions.JSSourceFunction
import com.basho.riak.client.query.indexes.KeyIndex
import com.basho.riak.client.query.{MapReduceResult, BucketMapReduce}
import com.basho.riak.client.raw.config.ClusterConfig
import com.basho.riak.client.raw.pbc.{PBClusterConfig, PBClusterClient, PBClientConfig}
import com.basho.riak.client.raw.query.indexes.BinRangeQuery
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.raw.RiakResponse



class RiakClient (hostPortsFile: String, hostPorts: Seq[(String,Int)]) {

	def this(hostPortsFile: String) = this(hostPortsFile, RiakClient.getHostPorts(hostPortsFile))
	
	val rkv = {
      val clusterConf = new PBClusterConfig(ClusterConfig.UNLIMITED_CONNECTIONS)
      hostPorts.map { hp => new PBClientConfig.Builder().withHost(hp._1).withPort(hp._2).build }
               .foreach { clusterConf.addClient(_) }
      new PBClusterClient(clusterConf)
	}
	
	override def toString() = hostPorts.mkString(",")
	
	def get(bucket: String, key: String, stopOnConflicts: Boolean): String = {
	    rkv.fetch(bucket, key) match {
	    	case r: RiakResponse if r.hasValue && r.hasSiblings => {	    		
	    		stopOnConflicts match {
	    			case false => r.getRiakObjects.max(new Ordering[IRiakObject] {
	    							def compare(x: IRiakObject, y: IRiakObject): Int = x.getLastModified compareTo y.getLastModified
	    						 }).getValueAsString
	    			case true => throw new Exception("Conflicts detected for '%s' in '%s'".format(key, bucket))
	    		}
	    	}
	    	case r: RiakResponse if r.hasValue => r.getRiakObjects()(0).getValueAsString
	    	case _ => null
	    }
	}
	
	def set(bucket: String, key: String, value: String) {  
		val item = RiakObjectBuilder.newBuilder(bucket, key).withValue(value).build
		rkv.store(item)
	}

	def del(bucket: String, key: String) = rkv.delete(bucket, key)
  
	def keysRange(bucket: String, from: String, to: String) : Buffer[String] = {
		val q = new BinRangeQuery(KeyIndex.index,  bucket, from, to)
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