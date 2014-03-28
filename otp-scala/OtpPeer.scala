package com.qifun.otp.erlang

case class OtpPeer(_node : String = "") extends AbstractNode(_node : String){
	var distChoose = 0
	def port() : Int = {
		OtpEpmd.lookupPort(this)
    }
}