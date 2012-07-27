package com.riak.utils

import scala.io._

object Keys {
	
	def generateKeyRanges(keysAlphabet: String, keysAlphabetEnding: String): Seq[(String,String)] = {
		val k = keysAlphabet.toSeq.sorted ++ keysAlphabetEnding
		k.zip(k.slice(1, k.length)).collect{ case (a,b) => (a.toString,b.toString) }.toSeq
	}

	def getKeysFromFile(keys: String, start: Int = 0, end: Int = Int.MaxValue) = Source.fromFile(keys).getLines.slice(start, end)

}