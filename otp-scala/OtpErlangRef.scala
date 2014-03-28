package com.qifun.otp.erlang

case class OtpErlangRef(node : String,refId : Array[Int],creation : Int) {
	
	def id() : Int = {
		refId(0)
	}
	
	def ids() : Array[Int] = {
		refId
	}
	
	def isNewRef() : Boolean = {
		refId.length > 1
	}
	
	def creations() : Int ={
		creation
	}
	
	def nodes() : String ={
		node
	}
}