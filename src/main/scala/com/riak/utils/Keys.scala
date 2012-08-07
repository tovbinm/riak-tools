package com.riak.utils

import scala.io._

object Keys {
	
	def generateKeyRanges(keysAlphabet: String, keysAlphabetEnding: String): Seq[(String,String)] = {
		val k = keysAlphabet.toSeq.sorted ++ keysAlphabetEnding
		generateKeyRanges(k)
	}

	def getKeysFromFile(keys: String) = Source.fromFile(keys).getLines
	
	def generateKeyRangesFromFile(keyPrefixes: String, keysPrefixesEnding: String): Seq[(String,String)] = {
	  val k = Source.fromFile(keyPrefixes).getLines.toSeq.sorted ++ keysPrefixesEnding
	  generateKeyRanges(k)
	}
	
	private def generateKeyRanges[T](k: Seq[T]): Seq[(String,String)] = {
	  k.zip(k.slice(1, k.length)).collect{ case (a,b) => (a.toString,b.toString) }.toSeq
	}

}