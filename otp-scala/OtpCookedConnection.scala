package com.qifun.otp.erlang

import java.net.Socket
import java.io.IOException

class OtpCookedConnection extends AbstractConnection {
   
	protected var cookSelf : OtpNode =null

    /*
     * The connection needs to know which local pids have links that pass
     * through here, so that they can be notified in case of connection failure
     */
    protected var links : Links  = null

    /**
     * Accept an incoming connection from a remote node. Used by {@link
     * OtpSelf#accept() OtpSelf.accept()} to create a connection based on data
     * received when handshaking with the peer node, when the remote node is the
     * connection intitiator
     */
    def initOtpCookedConnection(self : OtpNode , s : Socket) {
		abstractConnection(self, s)
		this.cookSelf = self
		links = Links(25)
		start()
    }

    /**
     * Intiate and open a connection to a remote node.
     * 
     * peer.
     */
    def initOtpCookedConnection(self : OtpNode ,other : OtpPeer) {
		abstractConnection(self, other)
		this.cookSelf = self
		links = Links(25)
		start()
    }

    // pass the error to the node
   override def deliver(e : Exception ) {
	   cookSelf.deliverError(this, e)
    }

    /*
     * pass the message to the node for final delivery. Note that the connection
     * itself needs to know about links (in case of connection failure), so we
     * snoop for link/unlink too here.
     */
    override def deliver(msg : OtpMsg) {
		val delivered = cookSelf.deliver(msg)
		msg.ntype() match {
			case OtpMsg.linkTag =>
			    if (delivered) {
			    	links.addLink(msg.getRecipientPid(), msg.getSenderPid())
			    } else {
					try {
					    // no such pid - send exit to sender
					    super.sendExit(msg.getRecipientPid(), msg.getSenderPid(),
						    Symbol("noproc"))
					} catch {
						case e : IOException =>
					}
			    }
			case (OtpMsg.unlinkTag | OtpMsg.exitTag)=>
			    links.removeLink(msg.getRecipientPid(), msg.getSenderPid())
			case OtpMsg.exit2Tag =>
			case _ => //println("msg tag = " + msg.ntype)
		}
    }

    /*
     * send to pid
     */
    def send(from : OtpErlangPid , dest: OtpErlangPid ,msg : Any){
	// encode and send the message
//    	println("@ enter send")
    	sendBuf(from, dest, OtpOutputStream(msg))
//    	println("@ exit send")
    }

    /*
     * send to remote name dest is recipient's registered name, the nodename is
     * implied by the choice of connection.
     */
    def send(from : OtpErlangPid , dest : String ,msg : Any) {
	// encode and send the message
    	sendBuf(from, dest, OtpOutputStream(msg))
    }

    override def close() {
		super.close()
		breakLinks()
    }

    override def finalize() {
    	close()
    }

    /*
     * this one called by dying/killed process
     */
    def exit(from : OtpErlangPid , to : OtpErlangPid ,reason : Any) {
		try {
		    super.sendExit(from, to, reason)
		} catch {
			case e : Exception =>
		}
    }

    /*
     * this one called explicitely by user code => use exit2
     */
    def exit2(from : OtpErlangPid , to : OtpErlangPid ,reason : Any) {
		try {
		    super.sendExit2(from, to, reason);
		} catch {
			case e : Exception =>
		}
    }

    /*
     * snoop for outgoing links and update own table
     */
     def link(from : OtpErlangPid , to : OtpErlangPid):Unit = synchronized{
		try {
		    super.sendLink(from, to)
		    links.addLink(from, to)
		} catch {
			case e : Exception =>
		    throw new OtpErlangExit("noproc")
		}
    }

    /*
     * snoop for outgoing unlinks and update own table
     */
    def unlink(from : OtpErlangPid , to : OtpErlangPid):Unit = synchronized{
		links.removeLink(from, to)
		try {
		    super.sendUnlink(from, to)
		} catch {
			case e : Exception =>
		}
    }

    /*
     * When the connection fails - send exit to all local pids with links
     * through this connection
     */
    def breakLinks() :Unit = synchronized{
	if (links != null) {
	    val l = links.clearLinks()  //Link[]

	    if (l != null) {
		val len = l.length

		for (i <- 0 until len) {
		    // send exit "from" remote pids to local ones
		    cookSelf.deliver(OtpMsg(OtpMsg.exitTag, l(i).remotes(), l(i).locals(), Symbol("noconnection")))
		}
	    }
	}
    }
}

object OtpCookedConnection{
	def apply(self : OtpNode , s : Socket) = {
		val conn = new OtpCookedConnection
		conn.initOtpCookedConnection(self, s)
		conn
	}
	
	def apply(self : OtpNode ,other : OtpPeer) = {
		val conn = new OtpCookedConnection
		conn.initOtpCookedConnection(self, other)
		conn
	}
}
