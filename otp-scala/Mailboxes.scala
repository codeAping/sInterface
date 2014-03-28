package com.qifun.otp.erlang

import java.io.IOException

/**
 * mail box 邮件队列
 */
class OtpMbox /*extends Thread*/{
	var home: OtpNode = null
	var mySelf: OtpErlangPid = null
	var queue: GenericQueue = null
	var name: String = null
	var links: Links = null

	var sendQueue: GenericQueue = null
	// package constructor: called by OtpNode:createMbox(name)
	// to create a named mbox
	def initOtpMbox(home: OtpNode, self: OtpErlangPid, name: String) {
		this.mySelf = self
		this.home = home
		this.name = name
		queue = new GenericQueue
		sendQueue = new GenericQueue
		queue.init
		links = Links(10)
//		setDaemon(true)
//		start()
	}

	// package constructor: called by OtpNode:createMbox()
	// to create an anonymous
	def initOtpMbox(home: OtpNode, self: OtpErlangPid) {
		initOtpMbox(home, self, null);
	}

	/**
	 * <p>
	 * Get the identifying {@link OtpErlangPid pid} associated with this
	 * mailbox.
	 * </p>
	 *
	 * <p>
	 * The {@link OtpErlangPid pid} associated with this mailbox uniquely
	 * identifies the mailbox and can be used to address the mailbox. You can
	 * send the {@link OtpErlangPid pid} to a remote communicating part so that
	 * he can know where to send his response.
	 * </p>
	 *
	 * @return the self pid for this mailbox.
	 */
	def self(): OtpErlangPid = {
		mySelf
	}

	/**
	 * <p>
	 * Register or remove a name for this mailbox. Registering a name for a
	 * mailbox enables others to send messages without knowing the
	 * {@link OtpErlangPid pid} of the mailbox. A mailbox can have at most one
	 * name; if the mailbox already had a name, calling this method will
	 * supercede that name.
	 * </p>
	 *
	 * @param name
	 *                the name to register for the mailbox. Specify null to
	 *                unregister the existing name from this mailbox.
	 *
	 * @return true if the name was available, or false otherwise.
	 */
	def registerName(name: String): Boolean = synchronized {
		home.registerName(name, this)
	}

	/**
	 * Get the registered name of this mailbox.
	 *
	 * @return the registered name of this mailbox, or null if the mailbox had
	 *         no registered name.
	 */
	def _getName(): String = {
		name
	}

	/**
	 * Block until a message arrives for this mailbox.
	 *
	 * @return an {@link OtpErlangObject OtpErlangObject} representing the body
	 *         of the next message waiting in this mailbox.
	 *
	 */
	def receive(): Any = {
		try {
			val s = System.currentTimeMillis();
			val msg = receiveMsg().getMsg()
			val e = System.currentTimeMillis();
//			System.err.println(queue.getCount + "=========" +msg)
//			if(e - s > 10)
//				System.out.println("=======================>"+(e - s) + "ms")
			return msg
		} catch {
			case e: Exception =>
				throw e
		}
	}

	/**
	 * Wait for a message to arrive for this mailbox.
	 *
	 * @param timeout
	 *                the time, in milliseconds, to wait for a message before
	 *                returning null.
	 *
	 * @return an {@link OtpErlangObject OtpErlangObject} representing the body
	 *         of the next message waiting in this mailbox.
	 */
	@throws(classOf[Exception])
	def receive(timeout: Long): Any = {
		try {
			val m = receiveMsg(timeout)
			if (m != null) {
				return m.getMsg()
			}
		} catch {
			case e: Exception =>
				throw e
		}
		return null
	}

	/**
	 * Block until a message arrives for this mailbox.
	 *
	 * @return a byte array representing the still-encoded body of the next
	 *         message waiting in this mailbox.
	 *
	 */
	def receiveBuf(): OtpInputStream = {
		receiveMsg().getMsgBuf()
	}

	/**
	 * Wait for a message to arrive for this mailbox.
	 *
	 * @param timeout
	 *                the time, in milliseconds, to wait for a message before
	 *                returning null.
	 *
	 * @return a byte array representing the still-encoded body of the next
	 *         message waiting in this mailbox.
	 */
	def receiveBuf(timeout: Long): OtpInputStream = {
		val m = receiveMsg(timeout)
		if (m != null) {
			return m.getMsgBuf()
		}

		return null
	}

	/**
	 * Block until a message arrives for this mailbox.
	 *
	 * @return an {@link OtpMsg OtpMsg} containing the header information as
	 *         well as the body of the next message waiting in this mailbox.
	 *
	 */
	def receiveMsg(): OtpMsg = {
		val m = queue.get().asInstanceOf[OtpMsg];
		m.ntype() match {
			case (OtpMsg.exitTag | OtpMsg.exit2Tag) =>
				try {
					val o = m.getMsg()
					throw new OtpErlangExit(o.toString)
				} catch {
					case e: Exception =>
						throw new OtpErlangExit("unknown")
				}

			case _ =>
				return m
		}
	}

	/**
	 * Wait for a message to arrive for this mailbox.
	 *
	 * @param timeout
	 *                the time, in milliseconds, to wait for a message.
	 *
	 * @return an {@link OtpMsg OtpMsg} containing the header information as
	 *         well as the body of the next message waiting in this mailbox.
	 */
	def receiveMsg(timeout: Long): OtpMsg = {
		val m = queue.get(timeout).asInstanceOf[OtpMsg]
		if (m == null) {
			return null
		}
		m.ntype() match {
			case (OtpMsg.exitTag | OtpMsg.exit2Tag) =>
				try {
					val o = m.getMsg()
					throw new OtpErlangExit(o.toString)
				} catch {
					case e: Exception =>
						throw new OtpErlangExit("unknown")
				}

			case _ =>
				return m
		}
	}

	/**
	 * Send a message to a remote {@link OtpErlangPid pid}, representing either
	 * another {@link OtpMbox mailbox} or an Erlang process.
	 *
	 * @param to
	 *                the {@link OtpErlangPid pid} identifying the intended
	 *                recipient of the message.
	 * @param msg
	 *                the body of the message to send.
	 *
	 */
	def send(to: OtpErlangPid, msg: Any) {
//		System.err.println("# -----" + msg)
		try {
			val node = to.node
			if (node == home.node) {
				home.deliver(OtpMsg(to, msg))
			} else {
				val conn = home.getConnection(node.name)
				if (conn == null) {
					return
				}
				conn.send(self, to, msg)
//				sendQueue.put((to,msg))
			}
//			System.err.println("send data time "+(e - s) + "ms");
		} catch {
			case e: Exception =>
				System.err.println("# send msg error, to node = " + to.node.name + " msg =" + msg.toString)
		}
	}

	def run(){
		println("# tcp send data thread start ...")
		while(true){
			var send = sendQueue.get
//			println(s"# $send==================" + sendQueue.getCount)
			if(send != null){
				var tmp = send.asInstanceOf[Tuple2[OtpErlangPid,Any]]
				val node = tmp._1.node
				val conn = home.getConnection(node.name)
				conn.send(self, tmp._1, tmp._2)
				tmp = null
			}
			send = null
		}
	}
	
	/**
	 * Send a message to a named mailbox created from the same node as this
	 * mailbox.
	 *
	 * @param name
	 *                the registered name of recipient mailbox.
	 * @param msg
	 *                the body of the message to send.
	 *
	 */
	def send(name: String, msg: Any) {
		home.deliver(OtpMsg(self, name, msg))
	}

	/**
	 * Send a message to a named mailbox created from another node.
	 *
	 * @param name
	 *                the registered name of recipient mailbox.
	 *
	 * @param node
	 *                the name of the remote node where the recipient mailbox is
	 *                registered.
	 *
	 * @param msg
	 *                the body of the message to send.
	 *
	 */
	def send(name: String, node: String, msg: Any) {
		val s = System.currentTimeMillis()
		try {
			val currentNode = home.nodes()
			if (node.equals(currentNode)) {
				send(name, msg)
			} else if (node.indexOf('@', 0) < 0
				&& node.equals(currentNode.substring(0, currentNode
					.indexOf('@', 0)))) {
				send(name, msg)
			} else {
				// other node
				val conn = home.getConnection(node);
				if (conn == null) {
					return
				}
				conn.send(self, name, msg)
			}
			val e = System.currentTimeMillis()
//			System.err.println("send data time "+(e - s) + "ms");
		} catch {
			case e: Exception =>
		}
	}

	/**
	 * Close this mailbox with the given reason.
	 *
	 * <p>
	 * After this operation, the mailbox will no longer be able to receive
	 * messages. Any delivered but as yet unretrieved messages can still be
	 * retrieved however.
	 * </p>
	 *
	 * <p>
	 * If there are links from this mailbox to other {@link OtpErlangPid pids},
	 * they will be broken when this method is called and exit signals will be
	 * sent.
	 * </p>
	 *
	 * @param reason
	 *                an Erlang term describing the reason for the exit.
	 */
	def exit(reason: Any) {
		home.closeMbox(this, reason)
	}

	/**
	 * Equivalent to <code>exit(new OtpErlangAtom(reason))</code>.
	 * </p>
	 *
	 * @see #exit(OtpErlangObject)
	 */
	def exit(reason: String) {
		exit(Symbol(reason))
	}

	/**
	 * <p>
	 * Send an exit signal to a remote {@link OtpErlangPid pid}. This method
	 * does not cause any links to be broken, except indirectly if the remote
	 * {@link OtpErlangPid pid} exits as a result of this exit signal.
	 * </p>
	 *
	 * @param to
	 *                the {@link OtpErlangPid pid} to which the exit signal
	 *                should be sent.
	 *
	 * @param reason
	 *                an Erlang term indicating the reason for the exit.
	 */
	// it's called exit, but it sends exit2
	def exit(to: OtpErlangPid, reason: Any) {
		exit(2, to, reason)
	}

	/**
	 * <p>
	 * Equivalent to <code>exit(to, new
	 * OtpErlangAtom(reason))</code>.
	 * </p>
	 *
	 * @see #exit(OtpErlangPid, OtpErlangObject)
	 */
	def exit(to: OtpErlangPid, reason: String) {
		exit(to, Symbol(reason))
	}

	// this function used internally when "process" dies
	// since Erlang discerns between exit and exit/2.
	private def exit(arity: Int, to: OtpErlangPid, reason: Any) {
		try {
			val node = to.node
			if (node == home.node) {
				home.deliver(OtpMsg(OtpMsg.exitTag, self, to, reason))
			} else {
				val conn = home.getConnection(node.name); //final OtpCookedConnection
				if (conn == null) {
					return
				}
				arity match {
					case 1 =>
						conn.exit(self, to, reason)
					case 2 =>
						conn.exit2(self, to, reason)
					case _ =>
						println("arity no match")
				}
			}
		} catch {
			case e: Exception =>
		}
	}

	/**
	 * <p>
	 * Link to a remote mailbox or Erlang process. Links are idempotent, calling
	 * this method multiple times will not result in more than one link being
	 * created.
	 * </p>
	 *
	 * <p>
	 * If the remote process subsequently exits or the mailbox is closed, a
	 * subsequent attempt to retrieve a message through this mailbox will cause
	 * an {@link OtpErlangExit OtpErlangExit} exception to be raised. Similarly,
	 * if the sending mailbox is closed, the linked mailbox or process will
	 * receive an exit signal.
	 * </p>
	 *
	 * <p>
	 * If the remote process cannot be reached in order to set the link, the
	 * exception is raised immediately.
	 * </p>
	 *
	 * @param to
	 *                the {@link OtpErlangPid pid} representing the object to
	 *                link to.
	 *
	 */
	def link(to: OtpErlangPid) {
		try {
			val node = to.node
			if (node == home.node) {
				if (!home.deliver(OtpMsg(OtpMsg.linkTag, self, to))) {
					throw new OtpErlangExit("noproc")
				}
			} else {
				val conn = home.getConnection(node.name); //final OtpCookedConnection 
				if (conn != null) {
					conn.link(self, to)
				} else {
					throw new OtpErlangExit("noproc")
				}
			}
		} catch {
			case e: OtpErlangExit =>
				throw e
		}

		links.addLink(self, to)
	}

	/**
	 * <p>
	 * Remove a link to a remote mailbox or Erlang process. This method removes
	 * a link created with {@link #link link()}. Links are idempotent; calling
	 * this method once will remove all links between this mailbox and the
	 * remote {@link OtpErlangPid pid}.
	 * </p>
	 *
	 * @param to
	 *                the {@link OtpErlangPid pid} representing the object to
	 *                unlink from.
	 *
	 */
	def unlink(to: OtpErlangPid) {
		links.removeLink(self, to)

		try {
			val node = to.node
			if (node == home.node) {
				home.deliver(OtpMsg(OtpMsg.unlinkTag, self, to))
			} else {
				val conn = home.getConnection(node.name); //final OtpCookedConnection
				if (conn != null) {
					conn.unlink(self, to)
				}
			}
		} catch {
			case e: Exception =>
		}
	}

	//    def ping(node : String , timeout : Long ) : Boolean = {
	//    	home.ping(node, timeout);
	//    }

	/**
	 * <p>
	 * Get a list of all known registered names on the same {@link OtpNode node}
	 * as this mailbox.
	 * </p>
	 *
	 * <p>
	 * This method calls a method with the same name in {@link OtpNode#getNames
	 * Otpnode} but is provided here for convenience.
	 * </p>
	 *
	 * @return an array of Strings containing all registered names on this
	 *         {@link OtpNode node}.
	 */
	def getNames(): Array[String] = {
		home.getNames()
	}

	/**
	 * Determine the {@link OtpErlangPid pid} corresponding to a registered name
	 * on this {@link OtpNode node}.
	 *
	 * <p>
	 * This method calls a method with the same name in {@link OtpNode#whereis
	 * Otpnode} but is provided here for convenience.
	 * </p>
	 *
	 * @return the {@link OtpErlangPid pid} corresponding to the registered
	 *         name, or null if the name is not known on this node.
	 */
	def whereis(name: String): OtpErlangPid = {
		home.whereis(name)
	}

	/**
	 * Close this mailbox.
	 *
	 * <p>
	 * After this operation, the mailbox will no longer be able to receive
	 * messages. Any delivered but as yet unretrieved messages can still be
	 * retrieved however.
	 * </p>
	 *
	 * <p>
	 * If there are links from this mailbox to other {@link OtpErlangPid pids},
	 * they will be broken when this method is called and exit signals with
	 * reason 'normal' will be sent.
	 * </p>
	 *
	 * <p>
	 * This is equivalent to {@link #exit(String) exit("normal")}.
	 * </p>
	 */
	def close() {
		home.closeMbox(this)
	}

	override protected def finalize() {
		close()
		queue.flush()
	}

	/**
	 * Determine if two mailboxes are equal.
	 *
	 * @return true if both Objects are mailboxes with the same identifying
	 *         {@link OtpErlangPid pids}.
	 */
	override def equals(o: Any): Boolean = {
		if (!(o.isInstanceOf[OtpMbox])) {
			return false
		}

		val m = o.asInstanceOf[OtpMbox]
		return m.self.equals(self)
	}

	/*
     * called by OtpNode to deliver message to this mailbox.
     * 
     * About exit and exit2: both cause exception to be raised upon receive().
     * However exit (not 2) causes any link to be removed as well, while exit2
     * leaves any links intact.
     */
	def deliver(m: OtpMsg) {
		m.ntype() match {
			case OtpMsg.linkTag =>
				links.addLink(self, m.getSenderPid())
			case OtpMsg.unlinkTag =>
				links.removeLink(self, m.getSenderPid())
			case OtpMsg.exitTag =>
				links.removeLink(self, m.getSenderPid())
				queue.put(m)
			case OtpMsg.exit2Tag =>
				queue.put(m)
			case _ =>
				queue.put(m)
		}
	}

	// used to break all known links to this mbox
	def breakLinks(reason: Any) {
		val l = links.clearLinks()

		if (l != null) {
			val len = l.length

			for (i <- 0 until len) {
				exit(1, l(i).remotes(), reason)
			}
		}
	}
}

object OtpMbox {
	def apply(home: OtpNode, self: OtpErlangPid, name: String) = {
		val otpMbox = new OtpMbox
		otpMbox.initOtpMbox(home, self, name)
		otpMbox
	}

	def apply(home: OtpNode, self: OtpErlangPid) = {
		val otpMbox = new OtpMbox
		otpMbox.initOtpMbox(home, self)
		otpMbox
	}
}
