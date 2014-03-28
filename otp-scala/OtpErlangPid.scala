package com.qifun.otp.erlang

/**
 *
 * 
 * provides a scala representation of Erlang PIDs. PIDs represent Erlang
 * processes and consist of a nodename and a number of integers.
 */
case class OtpErlangPid(node : Symbol, id : Int, serial : Int, creation : Int) {

	def toErlangString : String = {
//    	"#Pid<" + id + "." + serial + "." + creation + ">"
		"#Pid<" + node.name + "." + id + "." + serial + ">"
  	}
  
  	override def toString : String = {
//    	"#Pid<" + id + "." + serial + "." + creation + ">"
    	"#Pid<" + node.name + "." + id + "." + serial + ">"
    }
  
    override def equals(o : Any) : Boolean ={
    	if (!(o.isInstanceOf[OtpErlangPid])){
    		return false;
    	}
    	val pid = o.asInstanceOf[OtpErlangPid];
    	return (creation == pid.creation && serial == pid.serial && id == pid.id && node == pid.node)
    }
}
