package com.riak.utils

object Keys {
	
	def generateKeyRanges(keysAlphabet: String, keysAlphabetEnding: String): Seq[(String,String)] = {
		val k = keysAlphabet.toSeq.sorted ++ keysAlphabetEnding
		k.zip(k.slice(1, k.length)).collect{ case (a,b) => (a.toString,b.toString) }.toSeq
	}

}