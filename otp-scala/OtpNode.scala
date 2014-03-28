package com.qifun.otp.erlang

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Iterator;
import scala.util.control.Breaks._
/**
 * otp 节点管理
 */
object OtpNode {
	def apply(node: String) = {
		val nodes = new OtpNode
		nodes._otpNode(node)
		nodes
	}

	def apply(node: String, cookie: String) = {
		val nodes = new OtpNode
		nodes._otpNode(node, cookie)
		nodes
	}

	def apply(node: String, cookie: String, port: Int) = {
		val nodes = new OtpNode
		nodes._otpNode(node, cookie, port)
		nodes
	}
}

class OtpNode extends OtpLocalNode {
	private var initDone = false;

	// thread to manage incoming connections
	private var acceptor: Acceptor = null

	// keep track of all connections
	var connections: Hashtable[String, OtpCookedConnection] = null

	// keep track of all mailboxes
	var mboxes: Mailboxes = null

	// handle status changes
	var handler: OtpNodeStatus = null

	// flags
	var nodeFlags = 0

	/**
	 * <p>
	 * Create a node using the default cookie. The default cookie is found by
	 * reading the first line of the .erlang.cookie file in the user's home
	 * directory. The home directory is obtained from the System property
	 * "user.home".
	 * </p>
	 *
	 * <p>
	 * If the file does not exist, an empty string is used. This method makes no
	 * attempt to create the file.
	 * </p>
	 *
	 * @param node
	 *            the name of this node.
	 *
	 */
	def _otpNode(node: String) {
		_otpNode(node, defaultCookie, 0)
	}

	/**
	 * Create a node.
	 *
	 * @param node
	 *            the name of this node.
	 *
	 * @param cookie
	 *            the authorization cookie that will be used by this node when
	 *            it communicates with other nodes.
	 *
	 */
	def _otpNode(node: String, cookie: String) {
		_otpNode(node, cookie, 0)
	}

	/**
	 * Create a node.
	 *
	 * @param node
	 *            the name of this node.
	 *
	 * @param cookie
	 *            the authorization cookie that will be used by this node when
	 *            it communicates with other nodes.
	 *
	 * @param port
	 *            the port number you wish to use for incoming connections.
	 *            Specifying 0 lets the system choose an available port.
	 *
	 */
	def _otpNode(node: String, cookie: String, port: Int) {
		_otpLocalNode(node, cookie)
		init(port)
	}

	private def init(port: Int): Unit = synchronized {
		if (!initDone) {
			connections = new Hashtable[String, OtpCookedConnection](17, 0.95f)
			mboxes = Mailboxes(0)
			acceptor = Acceptor(port)
			initDone = true
		}
	}

	/**
	 * Close the node. Unpublish the node from Epmd (preventing new connections)
	 * and close all existing connections.
	 */
	def close(): Unit = synchronized {
		acceptor.quit()
		var conn: OtpCookedConnection = null
		val coll = connections.values()
		val it = coll.iterator()

		mboxes.clear()

		while (it.hasNext()) {
			conn = it.next()
			it.remove()
			conn.close()
		}
		initDone = false
	}

	override protected def finalize() {
		close()
	}

	/**
	 * Create an unnamed {@link OtpMbox mailbox} that can be used to send and
	 * receive messages with other, similar mailboxes and with Erlang processes.
	 * Messages can be sent to this mailbox by using its associated
	 * {@link OtpMbox#self() pid}.
	 *
	 * @return a mailbox.
	 */
	def createMbox(): OtpMbox = {
		mboxes.create()
	}

	/**
	 * Close the specified mailbox with reason 'normal'.
	 *
	 * @param mbox
	 *            the mailbox to close.
	 *
	 *            <p>
	 *            After this operation, the mailbox will no longer be able to
	 *            receive messages. Any delivered but as yet unretrieved
	 *            messages can still be retrieved however.
	 *            </p>
	 *
	 *            <p>
	 *            If there are links from the mailbox to other
	 *            {@link OtpErlangPid pids}, they will be broken when this
	 *            method is called and exit signals with reason 'normal' will be
	 *            sent.
	 *            </p>
	 *
	 */
	def closeMbox(mbox: OtpMbox) {
		closeMbox(mbox, Symbol("normal"))
	}

	/**
	 * Close the specified mailbox with the given reason.
	 *
	 * @param mbox
	 *            the mailbox to close.
	 * @param reason
	 *            an Erlang term describing the reason for the termination.
	 *
	 *            <p>
	 *            After this operation, the mailbox will no longer be able to
	 *            receive messages. Any delivered but as yet unretrieved
	 *            messages can still be retrieved however.
	 *            </p>
	 *
	 *            <p>
	 *            If there are links from the mailbox to other
	 *            {@link OtpErlangPid pids}, they will be broken when this
	 *            method is called and exit signals with the given reason will
	 *            be sent.
	 *            </p>
	 *
	 */
	def closeMbox(mbox: OtpMbox, reason: Any) {
		if (mbox != null) {
			mboxes.remove(mbox)
			mbox.name = null
			mbox.breakLinks(reason)
		}
	}

	/**
	 * Create an named mailbox that can be used to send and receive messages
	 * with other, similar mailboxes and with Erlang processes. Messages can be
	 * sent to this mailbox by using its registered name or the associated
	 * {@link OtpMbox#self() pid}.
	 *
	 * @param name
	 *            a name to register for this mailbox. The name must be unique
	 *            within this OtpNode.
	 *
	 * @return a mailbox, or null if the name was already in use.
	 *
	 */
	def createMbox(name: String): OtpMbox = {
		mboxes.create(name)
	}

	/**
	 * <p>
	 * Register or remove a name for the given mailbox. Registering a name for a
	 * mailbox enables others to send messages without knowing the
	 * {@link OtpErlangPid pid} of the mailbox. A mailbox can have at most one
	 * name; if the mailbox already had a name, calling this method will
	 * supercede that name.
	 * </p>
	 *
	 * @param name
	 *            the name to register for the mailbox. Specify null to
	 *            unregister the existing name from this mailbox.
	 *
	 * @param mbox
	 *            the mailbox to associate with the name.
	 *
	 * @return true if the name was available, or false otherwise.
	 */
	def registerName(name: String, mbox: OtpMbox): Boolean = {
		mboxes.register(name, mbox)
	}

	/**
	 * Get a list of all known registered names on this node.
	 *
	 * @return an array of Strings, containins all known registered names on
	 *         this node.
	 */

	def getNames(): Array[String] = {
		mboxes.names()
	}

	/**
	 * Determine the {@link OtpErlangPid pid} corresponding to a registered name
	 * on this node.
	 *
	 * @return the {@link OtpErlangPid pid} corresponding to the registered
	 *         name, or null if the name is not known on this node.
	 */
	def whereis(name: String): OtpErlangPid = {
		val m = mboxes.get(name)
		if (m != null) {
			return m.self()
		}
		return null
	}

	/**
	 * Register interest in certain system events. The {@link OtpNodeStatus
	 * OtpNodeStatus} handler object contains callback methods, that will be
	 * called when certain events occur.
	 *
	 * @param handler
	 *            the callback object to register. To clear the handler, specify
	 *            null as the handler to use.
	 *
	 */
	def registerStatusHandler(handler: OtpNodeStatus): Unit = synchronized {
		this.handler = handler
	}

	/**
	 * <p>
	 * Determine if another node is alive. This method has the side effect of
	 * setting up a connection to the remote node (if possible). Only a single
	 * outgoing message is sent; the timeout is how long to wait for a response.
	 * </p>
	 *
	 * <p>
	 * Only a single attempt is made to connect to the remote node, so for
	 * example it is not possible to specify an extremely long timeout and
	 * expect to be notified when the node eventually comes up. If you wish to
	 * wait for a remote node to be started, the following construction may be
	 * useful:
	 * </p>
	 *
	 * <pre>
	 * // ping every 2 seconds until positive response
	 * while (!me.ping(him, 2000))
	 *     ;
	 * </pre>
	 *
	 * @param node
	 *            the name of the node to ping.
	 *
	 * @param timeout
	 *            the time, in milliseconds, to wait for response before
	 *            returning false.
	 *
	 * @return true if the node was alive and the correct ping response was
	 *         returned. false if the correct response was not returned on time.
	 */
	/*
     * internal info about the message formats...
     * 
     * the request: -> REG_SEND {6,#Pid<bingo@aule.1.0>,'',net_kernel}
     * {'$gen_call',{#Pid<bingo@aule.1.0>,#Ref<bingo@aule.2>},{is_auth,bingo@aule}}
     * 
     * the reply: <- SEND {2,'',#Pid<bingo@aule.1.0>} {#Ref<bingo@aule.2>,yes}
     */
	def ping(node: String, timeout: Long): Boolean = {
		if (node.equals(this.node)) {
			return true
		} else if (node.indexOf('@', 0) < 0&& node.equals(this.node.substring(0, this.node.indexOf('@', 0)))) {
			return true
		}

		// other node
		var mbox: OtpMbox = null
		try {
			mbox = createMbox()
			mbox.send("net_kernel", node, getPingTuple(mbox))
			val reply = mbox.receive(timeout)

			val t = reply.asInstanceOf[Product]
			val a = t.productElement(1).asInstanceOf[Symbol]
			return "yes".equals(a.name)
		} catch {
			case e: Exception =>
		} finally {
			closeMbox(mbox)
		}
		return false
	}

	/* create the outgoing ping message */
	def getPingTuple(mbox: OtpMbox): Any = {
		val ping = new Array[Any](3)
		val pid = new Array[Any](2)
		val node = new Array[Any](2)

		pid(0) = mbox.self
		pid(1) = createRef()

		node(0) = Symbol("is_auth")
		node(1) = Symbol(nodes())

		ping(0) = Symbol("$gen_call")
		ping(1) = pid
		ping(2) = node
		ping
	}

	/*
     * this method simulates net_kernel only for the purpose of replying to
     * pings.
     */
	private def netKernel(m: OtpMsg): Boolean = {
		var mbox: OtpMbox = null
		try {
			val t = m.getMsg().asInstanceOf[Product]
			val req = t.productElement(1).asInstanceOf[Product] // actual
			// request

			val pid = req.productElement(0).asInstanceOf[OtpErlangPid] // originating
			// pid
			val pong = new Array[Any](2)
			pong(0) = req.productElement(1) // his #Ref
			pong(1) = Symbol("yes")

			mbox = createMbox()
			mbox.send(pid, pong)
			return true
		} catch {
			case e: Exception =>
		} finally {
			closeMbox(mbox)
		}
		return false
	}

	/*
     * OtpCookedConnection delivers messages here return true if message was
     * delivered successfully, or false otherwise.
     */
	def deliver(m: OtpMsg): Boolean = {
		var mbox: OtpMbox = null

		try {
			val t = m.ntype()

			if (t == OtpMsg.regSendTag) {
				val name = m.getRecipientName()
				/* special case for netKernel requests */
				if (name.equals("net_kernel")) {
					return netKernel(m)
				} else {
					mbox = mboxes.get(name)
				}
			} else {
				mbox = mboxes.get(m.getRecipientPid())
			}

			if (mbox == null) {
				return false
			}
			mbox.deliver(m);
		} catch {
			case e: Exception =>
				return false
		}

		return true
	}

	/*
     * OtpCookedConnection delivers errors here, we send them on to the handler
     * specified by the application
     */
	def deliverError(conn: OtpCookedConnection, e: Exception) {
		removeConnection(conn);
		remoteStatus(conn.name, false, e);
	}

	/*
     * find or create a connection to the given node
     */
	def getConnection(node: String): OtpCookedConnection = {
		var peer: OtpPeer = null
		var conn: OtpCookedConnection = null

		synchronized {
			// first just try looking up the name as-is
			conn = connections.get(node)

			if (conn == null) {
				// in case node had no '@' add localhost info and try again
				peer = new OtpPeer(node)
				conn = connections.get(peer.nodes())

				if (conn == null) {
					try {
						conn = OtpCookedConnection(this, peer)
						conn.setFlags(nodeFlags)
						addConnection(conn)
					} catch {
						case e: Exception =>
							connAttempt(peer.nodes(), false, e)
					}
				}
			}
			return conn
		}
	}

	def addConnection(conn: OtpCookedConnection) {
		if (conn != null && conn.name != null) {
			connections.put(conn.name, conn)
			remoteStatus(conn.name, true, null)
		}
	}

	private def removeConnection(conn: OtpCookedConnection) {
		if (conn != null && conn.name != null) {
			connections.remove(conn.name)
		}
	}

	/* use these wrappers to call handler functions */
	private def remoteStatus(node: String, up: Boolean,info: Object): Unit = synchronized {
		if (handler == null) {
			return
		}
		try {
			handler.remoteStatus(node, up, info)
		} catch {
			case e: Exception =>
		}
	}

	def localStatus(node: String, up: Boolean,info: Object): Unit = synchronized {
		if (handler == null) {
			return
		}
		try {
			handler.localStatus(node, up, info)
		} catch {
			case e: Exception =>
		}
	}

	def connAttempt(node: String, incoming: Boolean,info: Object): Unit = synchronized {
		if (handler == null) {
			return
		}
		try {
			handler.connAttempt(node, incoming, info);
		} catch {
			case e: Exception =>
		}
	}

	def setFlags(flags: Int) {
		this.nodeFlags = flags
	}

	////***/////////////////////////////////////////////////***///////////////////////////////////////////////
	object Mailboxes {
		def apply(i: Int) = {
			val boxs = new Mailboxes
			boxs._mailboxes
			boxs
		}
	}
	class Mailboxes {
		// mbox pids here
		var byPid: Hashtable[OtpErlangPid, WeakReference[OtpMbox]] = null
		// mbox names here
		var byName: Hashtable[String, WeakReference[OtpMbox]] = null

		def _mailboxes() {
			byPid = new Hashtable[OtpErlangPid, WeakReference[OtpMbox]](17, 0.95f)
			byName = new Hashtable[String, WeakReference[OtpMbox]](17, 0.95f)
		}

		def create(name: String): OtpMbox = {
			var m: OtpMbox = null

			synchronized {
				if (get(name) != null) {
					return null
				}
				val pid = createPid()
				m = OtpMbox(OtpNode.this, pid, name)
				byPid.put(pid, new WeakReference[OtpMbox](m))
				byName.put(name, new WeakReference[OtpMbox](m))
			}
			println("# mail box create success")
			return m
		}

		def create(): OtpMbox = {
			val pid = createPid()
			val m = OtpMbox(OtpNode.this, pid)
			byPid.put(pid, new WeakReference[OtpMbox](m))
			return m
		}

		def clear() {
			byPid.clear()
			byName.clear()
		}

		def names(): Array[String] = {
			var allnames: Array[String] = null;

			synchronized {
				val n = byName.size()
				val keys = byName.keys()
				allnames = new Array[String](n)

				var i = 0
				while (keys.hasMoreElements()) {
					allnames(i) = keys.nextElement()
					i += 1
				}
			}
			allnames
		}

		def register(name: String, mbox: OtpMbox): Boolean = {
			if (name == null) {
				if (mbox.name != null) {
					byName.remove(mbox.name)
					mbox.name = null
				}
			} else {
				synchronized {
					if (get(name) != null) {
						return false
					}
					byName.put(name, new WeakReference[OtpMbox](mbox));
					mbox.name = name
				}
			}
			return true
		}

		/*
		 * look up a mailbox based on its name. If the mailbox has gone out of
		 * scope we also remove the reference from the hashtable so we don't
		 * find it again.
		 */
		def get(name: String): OtpMbox = {
			val wr = byName.get(name)

			if (wr != null) {
				val m = wr.get()
				if (m != null) {
					return m
				}
				byName.remove(name)
			}
			return null
		}

		/*
		 * look up a mailbox based on its pid. If the mailbox has gone out of
		 * scope we also remove the reference from the hashtable so we don't
		 * find it again.
		 */
		def get(pid: OtpErlangPid): OtpMbox = {
			val wr = byPid.get(pid)

			if (wr != null) {
				val m = wr.get()

				if (m != null) {
					return m
				}
				byPid.remove(pid)
			}
			return null
		}

		def remove(mbox: OtpMbox) {
			byPid.remove(mbox.self)
			if (mbox.name != null) {
				byName.remove(mbox.name)
			}
		}
	}

	/*
     * this thread simply listens for incoming connections
     */
	object Acceptor {
		def apply(port: Int) = {
			val accept = new Acceptor(port)
			accept.startAccept
			accept
		}
	}
	class Acceptor(port: Int) extends Thread {
		var sock: ServerSocket = null
		var done = false
		def startAccept() {
			sock = new ServerSocket(port)
			val tmpPort = sock.getLocalPort()
			OtpNode.this.port = tmpPort
			setDaemon(true)
			setName("acceptor")
			publishPort()
			start()
			println(s"# start node success @ $port")
		}

		def publishPort(): Boolean = {
			if (getEpmd() != null) {
				return false; // already published
			}
			OtpEpmd.publishPort(OtpNode.this)
			return true
		}

		def unPublishPort() {
			// unregister with epmd
			OtpEpmd.unPublishPort(OtpNode.this)

			// close the local descriptor (if we have one)
			closeSock(epmd)
			epmd = null
		}

		def quit() {
			unPublishPort()
			done = true
			closeSock(sock)
			localStatus(node, false, null)
		}

		private def closeSock(s: ServerSocket) {
			try {
				if (s != null) {
					s.close()
				}
			} catch {
				case e: Exception =>
			}
		}

		private def closeSock(s: Socket) {
			try {
				if (s != null) {
					s.close()
				}
			} catch {
				case e: Exception =>
			}
		}

		def port(): Int = {
			port
		}

		override def run() {
			var newsock: Socket = null
			var conn: OtpCookedConnection = null

			localStatus(node, true, null)
			breakable {
				while (!done) {
					conn = null
					try {
						newsock = sock.accept();
					} catch {
						case e: Exception =>
							// accept throws SocketException
							// when socket is closed. This will happen when
							// acceptor.quit()
							// is called. acceptor.quit() will call localStatus(...), so
							// we have to check if that's where we come from.
							if (!done) {
								localStatus(node, false, e)
							}
							break
					}

					try {
						synchronized {
							conn = OtpCookedConnection(OtpNode.this, newsock)
							conn.setFlags(nodeFlags)
							addConnection(conn)
						}
					} catch {
						case e: IOException =>
							println(e.getMessage())
							if (conn != null && conn.name != null) {
								connAttempt(conn.name, true, e)
							} else {
								connAttempt("unknown", true, e)
							}
							closeSock(newsock)
						case e: Exception =>
							closeSock(newsock)
							closeSock(sock)
							localStatus(node, false, e)
							break

					}
				} // while
			}
			// if we have exited loop we must do this too
			unPublishPort()
		}
	}
}


