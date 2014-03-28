package com.qifun.otp.erlang

import java.io.IOException;
import java.net.Socket;
import com.qifun.otp.erlang._

object OtpConnection {
	def apply(self : OtpSelf , s : Socket) ={
		val otpConn = new OtpConnection()
		otpConn._otpConnection(self, s)
		otpConn
	}
	
	def apply(self : OtpSelf , other : OtpPeer ) ={
		val otpConn = new OtpConnection()
		otpConn._otpConnection(self, other)
		otpConn
	}
}


class OtpConnection extends AbstractConnection {
    protected var mySelf : OtpSelf = null
    protected var queue : GenericQueue = null // messages get delivered here
    // package scope
    def _otpConnection(self : OtpSelf , s : Socket) {
		abstractConnection(self, s)
		this.mySelf = self
		queue = new GenericQueue
		queue.init
		setDaemon(true)
		start()
    }

    /*
     * Intiate and open a connection to a remote node.
     * 
     * @exception java.io.IOException if it was not possible to connect to the
     * peer.
     * 
     * @exception OtpAuthException if handshake resulted in an authentication
     * error.
     */
    def _otpConnection(self : OtpSelf , other : OtpPeer){
		abstractConnection(self, other)
		this.mySelf = self
		queue = new GenericQueue
		queue.init
		start()
    }

    override def deliver(e : Exception ) {
    	queue.put(e)
    }

    override def deliver(msg : OtpMsg) {
    	queue.put(msg)
    }

    /**
     * Get information about the node at the peer end of this connection.
     * 
     * @return the {@link OtpPeer Node} representing the peer node.
     */
    def  peers() : OtpPeer ={
    	peer
    }

    /**
     * Get information about the node at the local end of this connection.
     * 
     * @return the {@link OtpSelf Node} representing the local node.
     */
    def mySelfs() : OtpSelf = {
    	mySelf
    }

    /**
     * Return the number of messages currently waiting in the receive queue for
     * this connection.
     */
    def msgCount() : Int ={
    	queue.getCount()
    }

    /**
     * Receive a message from a remote process. This method blocks until a valid
     * message is received or an exception is raised.
     * 
     * <p>
     * If the remote node sends a message that cannot be decoded properly, the
     * connection is closed and the method throws an exception.
     * 
     * @return an object containing a single Erlang term.
     */
    @throws(classOf[OtpErlangExit])
    def receive() : Any ={ //OtpErlangObject
		try {
		    return receiveMsg().getMsg()
		} catch  {
			case e : Exception =>
		    close();
		    throw new IOException(e.getMessage())
		}
    }

    /**
     * Receive a message from a remote process. This method blocks at most for
     * the specified time, until a valid message is received or an exception is
     * raised.
     * 
     * <p>
     * If the remote node sends a message that cannot be decoded properly, the
     * connection is closed and the method throws an exception.
     * 
     * @param timeout
     *                the time in milliseconds that this operation will block.
     *                Specify 0 to poll the queue.
     * 
     * @return an object containing a single Erlang term.
     */
    @throws(classOf[OtpErlangExit])
    def receive(timeout : Long) : Any = {
		try {
		    return receiveMsg(timeout).getMsg()
		} catch {
			case e : Exception =>
		    close()
		    throw new IOException(e.getMessage())
		}
    }

    /**
     * Receive a raw (still encoded) message from a remote process. This message
     * blocks until a valid message is received or an exception is raised.
     * 
     * <p>
     * If the remote node sends a message that cannot be decoded properly, the
     * connection is closed and the method throws an exception.
     * 
     * @return an object containing a raw (still encoded) Erlang term.
     */
    def receiveBuf() : OtpInputStream ={
    	receiveMsg().getMsgBuf()
    }

    /**
     * Receive a raw (still encoded) message from a remote process. This message
     * blocks at most for the specified time until a valid message is received
     * or an exception is raised.
     * 
     * <p>
     * If the remote node sends a message that cannot be decoded properly, the
     * connection is closed and the method throws an exception.
     * 
     * @param timeout
     *                the time in milliseconds that this operation will block.
     *                Specify 0 to poll the queue.
     * 
     * @return an object containing a raw (still encoded) Erlang term.
     */
    def  receiveBuf(timeout : Long): OtpInputStream = {
    	receiveMsg(timeout).getMsgBuf()
    }

    /**
     * Receive a messge complete with sender and recipient information.
     * 
     * @return an {@link OtpMsg OtpMsg} containing the header information about
     *         the sender and recipient, as well as the actual message contents.
     */
    @throws(classOf[OtpErlangExit])
    def receiveMsg():OtpMsg = {
		val o = queue.get()
	
		if (o.isInstanceOf[OtpMsg]) {
		    return  o.asInstanceOf[OtpMsg]
		} else if (o.isInstanceOf[IOException]) {
		    throw o.asInstanceOf[IOException]
		} else if (o.isInstanceOf[OtpErlangExit]) {
		    throw o.asInstanceOf[OtpErlangExit]
		} 
		return null
    }

    /**
     * Receive a messge complete with sender and recipient information. This
     * method blocks at most for the specified time.
     * 
     * @param timeout
     *                the time in milliseconds that this operation will block.
     *                Specify 0 to poll the queue.
     * 
     * @return an {@link OtpMsg OtpMsg} containing the header information about
     *         the sender and recipient, as well as the actual message contents.
     */
    @throws(classOf[OtpErlangExit])
    def receiveMsg(timeout: Long) : OtpMsg = {
    	val o = queue.get(timeout)

		if (o.isInstanceOf[OtpMsg]) {
		    return  o.asInstanceOf[OtpMsg]
		} else if (o.isInstanceOf[IOException]) {
		    throw o.asInstanceOf[IOException]
		} else if (o.isInstanceOf[OtpErlangExit]) {
		    throw o.asInstanceOf[OtpErlangExit]
		}
		return null
    }

    /**
     * Send a message to a process on a remote node.
     * 
     * @param dest
     *                the Erlang PID of the remote process.
     * @param msg
     *                the message to send.
     */
    def send(dest : OtpErlangPid , msg : Any) {
	// encode and send the message
    	super.sendBuf(mySelf.pids(), dest, OtpOutputStream(msg))
    }

    /**
     * Send a message to a named process on a remote node.
     * 
     * @param dest
     *                the name of the remote process.
     * @param msg
     *                the message to send.
     */
    def send(dest : String , msg : Any ) {
	// encode and send the message
    	super.sendBuf(mySelf.pids(), dest, OtpOutputStream(msg))
    }

    /**
     * Send a pre-encoded message to a named process on a remote node.
     * 
     * @param dest
     *                the name of the remote process.
     * @param payload
     *                the encoded message to send.
     */
    def sendBuf(dest : String , payload : OtpOutputStream) {
    	super.sendBuf(mySelf.pids(), dest, payload)
    }

    /**
     * Send a pre-encoded message to a process on a remote node.
     * 
     * @param dest
     *                the Erlang PID of the remote process.
     * @param msg
     *                the encoded message to send.
     */
    def sendBuf(dest : OtpErlangPid, payload : OtpOutputStream){
    	super.sendBuf(mySelf.pids(), dest, payload)
    }


    /**
     * Send an RPC request to the remote Erlang node. This convenience function
     * creates the following message and sends it to 'rex' on the remote node:
     */
    def sendRPC(mod : String, fun : String ,args : Array[Any]){
		val rpc = new Array[Any](2)
		val call = new Array[Any](5)
	
		/* {self, { call, Mod, Fun, Args, user}} */
	
		call(0) = Symbol("call")
		call(1) = Symbol(mod)
		call(2) = Symbol(fun)
		call(3) = args
		call(4) = Symbol("user")
	
		rpc(0) = mySelf.pids()
		rpc(1) = Tuple5(call(0),call(1),call(2),call(3),call(4))
	
		send("rex", Tuple2(rpc(0),rpc(1)))
    }

    /**
     * Create a link between the local node and the specified process on the
     * remote node. If the link is still active when the remote process
     * terminates, an exit signal will be sent to this connection. Use
     * {@link #unlink unlink()} to remove the link.
     * 
     * @param dest
     *                the Erlang PID of the remote process.
     * 
     * @exception java.io.IOException
     *                    if the connection is not active or a communication
     *                    error occurs.
     */
    def link(dest : OtpErlangPid ) {
    	super.sendLink(mySelf.pids(), dest)
    }

    /**
     * Remove a link between the local node and the specified process on the
     * remote node. This method deactivates links created with
     * {@link #link link()}.
     * 
     * @param dest
     *                the Erlang PID of the remote process.
     */
    def unlink(dest : OtpErlangPid) {
    	super.sendUnlink(mySelf.pids(), dest)
    }

    /**
     * Send an exit signal to a remote process.
     * 
     * @param dest
     *                the Erlang PID of the remote process.
     * @param reason
     *                an Erlang term describing the exit reason.
     */
    def exit(dest : OtpErlangPid ,reason:Any ) {
    	super.sendExit2(mySelf.pids(), dest, reason)
    }
}