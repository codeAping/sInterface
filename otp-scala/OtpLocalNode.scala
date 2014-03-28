package com.qifun.otp.erlang

object OtpLocalNode{
	def apply = {
		val node = new OtpLocalNode
		node._otpLocalNode
		node
	}
	
	def apply(nodes : String) = {
		val node = new OtpLocalNode(nodes)
		node._otpLocalNode(nodes)
		node
	}
	
	def apply(nodes : String , cookies : String ) = {
		val node = new OtpLocalNode(nodes,cookies)
		node._otpLocalNode(nodes,cookies)
		node
	}
}


class OtpLocalNode(nodes: String = "", cookies: String = "") extends AbstractNode(nodes, cookies) {
    var serial = 0
    var pidCount = 1
    var portCount = 1
    var refId : Array[Int] = null

    var port : Int = 0
    var epmd : java.net.Socket = null

    def _otpLocalNode() {
		init()
    }
//
//    /**
//     * Create a node with the given name and the default cookie.
//     */
    def _otpLocalNode(node : String) {
		abstractNode(node)
		init()
    }
//
//    /**
//     * Create a node with the given name and cookie.
//     */
    def _otpLocalNode(node : String , cookie : String ) {
		abstractNode(node, cookie)
		init()
    }

    /**
     * 初始化网络环境
     */
    private def init() {
		serial = 0
		pidCount = 1
		portCount = 1
		refId = Array.apply(1,0,0)
    }

    /**
     * Get the port number used by this node.
     * 
     * @return the port number this server node is accepting connections on.
     */
    def  ports() : Int = {
    	port
    }

    /**
     * Set the Epmd socket after publishing this nodes listen port to Epmd.
     * 
     * @param s
     *                The socket connecting this node to Epmd.
     */
    def setEpmd(s : java.net.Socket ) {
    	epmd = s
    }

    /**
     * Get the Epmd socket.
     * 
     * @return The socket connecting this node to Epmd.
     */
    def  getEpmd() : java.net.Socket ={
    	epmd
    }

    /**
     * Create an Erlang {@link OtpErlangPid pid}. Erlang pids are based upon
     * some node specific information; this method creates a pid using the
     * information in this node. Each call to this method produces a unique pid.
     * 
     * @return an Erlang pid.
     */
    
	def createPid() : OtpErlangPid = synchronized{
		val p = OtpErlangPid(Symbol(node), pidCount, serial,creation)
		pidCount += 1
		if (pidCount > 0x7fff) {
		    pidCount = 0
		    serial += 1
		    if (serial > 0x1fff) { /* 13 bits */
		    	serial = 0
		    }
		}
		p
	}

    /**
     * Create an Erlang {@link OtpErlangPort port}. Erlang ports are based upon
     * some node specific information; this method creates a port using the
     * information in this node. Each call to this method produces a unique
     * port. It may not be meaningful to create a port in a non-Erlang
     * environment, but this method is provided for completeness.
     * 
     * @return an Erlang port.
     */
    
	def createPort() : OtpErlangPort =synchronized{
		val p = OtpErlangPort(Symbol(node), portCount, creation)
		portCount += 1
		if (portCount > 0xfffffff) { /* 28 bits */
			portCount = 0
		}
		p
	}

    /**
     * Create an Erlang {@link OtpErlangRef reference}. Erlang references are
     * based upon some node specific information; this method creates a
     * reference using the information in this node. Each call to this method
     * produces a unique reference.
     * 
     * @return an Erlang reference.
     */
	 def createRef() : OtpErlangRef = synchronized{
		val r = OtpErlangRef(node, refId, creation)
		refId(0) += 1
		if (refId(0) > 0x3ffff) {
		    refId(0) = 0
		    refId(1) += 1
		    if (refId(1) == 0) {
		    	refId(2)+=1
		    }
		}
		r
	 }
}
