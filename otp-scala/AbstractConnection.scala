package com.qifun.otp.erlang

import java.io.IOException
import java.io.InputStream
import java.net.Socket
import java.util.Random
import scala.util.control.Breaks._
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * socket 连接 基类
 * Accept an incoming connection from a remote node.
 * used by {@link OtpSelf#accept OtpSelf.accept()} to create a connection based on data
 * received when handshaking with the peer node
 */

abstract class AbstractConnection extends Thread {
	val headerLen = 2048; // more than enough

	val passThrough = (0x70).asInstanceOf[Byte];
	val version = (0x83).asInstanceOf[Byte];

	// Erlang message header tags
	protected val linkTag = 1;
	protected val sendTag = 2;
	protected val exitTag = 3;
	protected val unlinkTag = 4;
	protected val regSendTag = 6;
	protected val groupLeaderTag = 7;
	protected val exit2Tag = 8;

	protected val sendTTTag = 12;
	protected val exitTTTag = 13;
	protected val regSendTTTag = 16;
	protected val exit2TTTag = 18;

	// MD5 challenge messsage tags
	protected val ChallengeReply = 'r'.asInstanceOf[Int]; ;
	protected val ChallengeAck = 'a'.asInstanceOf[Int];
	protected val ChallengeStatus = 's'.asInstanceOf[Int]; ;

	private var done = false;

	protected var connected = false; // connection status
	protected var socket: Socket = null // communication channel
	protected var peer: OtpPeer = null // who are we connected to
	protected var self: OtpLocalNode = null // this nodes id

	val lock = new ReentrantLock
	val lockk = new ReentrantReadWriteLock
	val readLock = lockk.readLock()
	val writeLock = lockk.writeLock()
	var name: String = "" // local name of this connection

	protected var cookieOk = false; // already checked the cookie for this
	// connection
	protected var sendCookie = true; // Send cookies in messages?

	// tracelevel constants
	protected var traceLevel = 0;

	protected var defaultLevel = 0;
	protected var sendThreshold = 1;
	protected var ctrlThreshold = 2;
	protected var handshakeThreshold = 3;

	protected var random: Random = new Random();

	private var flags = 0;

	{
		// trace this connection?
		var trace = System.getProperties().getProperty("OtpConnection.trace");
		try {
			if (trace != null) {
				defaultLevel = Integer.valueOf(trace).intValue();
			}
		} catch {
			case e: NumberFormatException => defaultLevel = 0;
		}
	}

	// private AbstractConnection() {
	// }

	def abstractConnection(self: OtpLocalNode, s: Socket) {
		this.self = self;
		peer = new OtpPeer();
		socket = s;
		socket.setTcpNoDelay(true);
		traceLevel = defaultLevel;
		setDaemon(true);

		if (traceLevel >= handshakeThreshold) {
			System.out.println("<- ACCEPT FROM " + s.getInetAddress() + ":" + s.getPort());
		}

		// get his info
		recvName(peer);

		// now find highest common dist value
		if (peer.proto != self.proto || self.distHigh < peer.distLow || self.distLow > peer.distHigh) {
			close();
			throw new IOException(
				"No common protocol found - cannot accept connection");
		}
		// highest common version: min(peer.distHigh, self.distHigh)

		if (peer.distHigh > self.distHigh)
			peer.distChoose = self.distHigh
		else
			peer.distChoose = peer.distHigh

		doAccept();

		name = peer.nodes();
	}

	/**
	 * init and open a connection to a remote node.
	 *
	 */
	def abstractConnection(self: OtpLocalNode, other: OtpPeer) {
		peer = other;
		this.self = self;
		socket = null;
		var port = 0;

		traceLevel = defaultLevel;
		setDaemon(true);

		// now get a connection between the two...
		port = OtpEpmd.lookupPort(peer);

		// now find highest common dist value
		if (peer.proto != self.proto || self.distHigh < peer.distLow|| self.distLow > peer.distHigh) {
			throw new IOException("No common protocol found - cannot connect");
		}

		// highest common version: min(peer.distHigh, self.distHigh)
		if (peer.distHigh > self.distHigh)
			peer.distChoose = self.distHigh
		else
			peer.distChoose = peer.distHigh;

		doConnect(port);

		name = peer.nodes();
		connected = true;
	}

	/**
	 * Deliver communication exceptions to the recipient.
	 */
	def deliver(e: Exception);

	/**
	 * Deliver messages to the recipient.
	 */
	def deliver(msg: OtpMsg);

	/**
	 * Send a pre-encoded message to a named process on a remote node.
	 *
	 * @param dest
	 *            the name of the remote process.
	 * @param payload
	 *            the encoded message to send.
	 */
	def sendBuf(from: OtpErlangPid, dest: String, payload: OtpOutputStream) {
		if (!connected) {
			throw new IOException("Not connected");
		}
		val header = OtpOutputStream(headerLen);

		// preamble: 4 byte length + "passthrough" tag + version
		header.write4BE(0); // reserve space for length
		header.write1(passThrough);
		header.write1(version);

		// header info
		header.write_tuple_head(4);
		header.write_long(regSendTag);
		header.write_any(from);
		if (sendCookie) {
			header.write_atom(self.cookies());
		} else {
			header.write_atom("");
		}
		header.write_atom(dest);

		// version for payload
		header.write1(version);

		// fix up length in preamble
		header.poke4BE(0, header.size() + payload.size() - 4);

		do_send(header, payload);
	}

	/**
	 * Send a pre-encoded message to a process on a remote node.
	 *
	 * @param dest
	 *            the Erlang PID of the remote process.
	 * @param msg
	 *            the encoded message to send.
	 */
	def sendBuf(from: OtpErlangPid, dest: OtpErlangPid, payload: OtpOutputStream) {
		if (!connected) {
			throw new IOException("Not connected")
		}
		val header = OtpOutputStream(headerLen)

		// preamble: 4 byte length + "passthrough" tag + version
		header.write4BE(0) // reserve space for length
		header.write1(passThrough)
		header.write1(version)

		// header info
		header.write_tuple_head(3)
		header.write_long(sendTag)
		if (sendCookie) {
			header.write_atom(self.cookies())
		} else {
			header.write_atom("")
		}
		header.write_any(dest)

		// version for payload
		header.write1(version)

		// fix up length in preamble
		header.poke4BE(0, header.size() + payload.size() - 4)

		do_send(header, payload)
	}

	/*
     * Send an auth error to peer because he sent a bad cookie. The auth error
     * uses his cookie (not revealing ours). This is just like send_reg
     * otherwise
     */
	private def cookieError(local: OtpLocalNode, cookie: Symbol) {
		try {
			val header = OtpOutputStream(headerLen)

			// preamble: 4 byte length + "passthrough" tag + version
			header.write4BE(0); // reserve space for length
			header.write1(passThrough)
			header.write1(version)

			header.write_tuple_head(4)
			header.write_long(regSendTag)
			header.write_any(local.createPid()) // disposable pid
			header.write_atom(cookie) // important: his cookie,
			// not mine...
			header.write_atom("auth")

			// version for payload
			header.write1(version)

			// the payload

			// the no_auth message (copied from Erlang) Don't change this
			// (Erlang will crash)
			// {$gen_cast, {print, "~n** Unauthorized cookie ~w **~n",
			// [foo@aule]}}
			val msg = new Array[Any](2)
			val msgbody = new Array[Any](3)

			msgbody(0) = Symbol("print")
			msgbody(1) = new String("~n** Bad cookie sent to " + local + " **~n")
			// Erlang will crash and burn if there is no third argument here...
			msgbody(2) = List() // empty list

			msg(0) = Symbol("$gen_cast")
			msg(1) = Tuple1(msgbody)

			val payload = OtpOutputStream(Tuple1(msg))
//			payload.write_any(msg)

			// fix up length in preamble
			header.poke4BE(0, header.size() + payload.size() - 4)

			try {
				do_send(header, payload)
			} catch {
				case e: IOException =>
			} // ignore
		} finally {
			close()
		}
		throw new Exception("Remote cookie not authorized: " + cookie.name)
	}

	// link to pid

	/**
	 * Create a link between the local node and the specified process on the
	 * remote node. If the link is still active when the remote process
	 * terminates, an exit signal will be sent to this connection. Use
	 * {@link #sendUnlink unlink()} to remove the link.
	 *
	 * @param dest
	 *            the Erlang PID of the remote process.
	 */
	protected def sendLink(from: OtpErlangPid, dest: OtpErlangPid) {
		if (!connected) {
			throw new IOException("Not connected")
		}
		val header = OtpOutputStream(headerLen)

		// preamble: 4 byte length + "passthrough" tag
		header.write4BE(0) // reserve space for length
		header.write1(passThrough)
		header.write1(version)

		// header
		header.write_tuple_head(3)
		header.write_long(linkTag)
		header.write_any(from)
		header.write_any(dest)

		// fix up length in preamble
		header.poke4BE(0, header.size() - 4)

		do_send(header)
	}

	/**
	 * Remove a link between the local node and the specified process on the
	 * remote node. This method deactivates links created with {@link #sendLink
	 * link()}.
	 *
	 * @param dest
	 *            the Erlang PID of the remote process.
	 */
	protected def sendUnlink(from: OtpErlangPid, dest: OtpErlangPid) {
		if (!connected) {
			throw new IOException("Not connected")
		}
		val header = OtpOutputStream(headerLen)

		// preamble: 4 byte length + "passthrough" tag
		header.write4BE(0) // reserve space for length
		header.write1(passThrough)
		header.write1(version)

		// header
		header.write_tuple_head(3)
		header.write_long(unlinkTag)
		header.write_any(from)
		header.write_any(dest)

		// fix up length in preamble
		header.poke4BE(0, header.size() - 4)

		do_send(header)
	}

	/* used internally when "processes" terminate */
	protected def sendExit(from: OtpErlangPid, dest: OtpErlangPid, reason: Any) {
		sendExit(exitTag, from, dest, reason)
	}

	/**
	 * Send an exit signal to a remote process.
	 *
	 * @param dest
	 *            the Erlang PID of the remote process.
	 * @param reason
	 *            an Erlang term describing the exit reason.
	 */
	protected def sendExit2(from: OtpErlangPid, dest: OtpErlangPid, reason: Any) {
		sendExit(exit2Tag, from, dest, reason)
	}

	private def sendExit(tag: Int, from: OtpErlangPid, dest: OtpErlangPid, reason: Any) {
		if (!connected) {
			throw new IOException("Not connected")
		}
		val header = OtpOutputStream(headerLen)

		// preamble: 4 byte length + "passthrough" tag
		header.write4BE(0) // reserve space for length
		header.write1(passThrough)
		header.write1(version)

		// header
		header.write_tuple_head(4)
		header.write_long(tag)
		header.write_any(from)
		header.write_any(dest)
		header.write_any(reason)

		// fix up length in preamble
		header.poke4BE(0, header.size() - 4)

		do_send(header)
	}

	override def run() {
		if (!connected) {
			deliver(new IOException("Not connected"))
			return
		}

		var lbuf = new Array[Byte](4)
		var ibuf: OtpInputStream = null
		var traceobj: Any = null
		//	OtpErlangObject traceobj;
		var len = 0
		var tock = new Array[Byte](4)
		tock(0) = 0
		tock(1) = 0
		tock(2) = 0
		tock(3) = 0

		try {
//			breakable {
				while (!done) {
//					breakable {
					do {
						readSock(socket, lbuf)
						ibuf = OtpInputStream(lbuf, flags)
						len = ibuf.read4BE()
						if (len == 0) {
//							synchronized {
							try{
								lock.lock()
								socket.getOutputStream().write(tock)
							}finally{
								lock.unlock();
							}
//							}
						}
					} while (len == 0) // tick_loop
//					}

					// got a real message (maybe) - read len bytes
					var tmpbuf = new Array[Byte](len)
					// i = socket.getInputStream().read(tmpbuf);
					readSock(socket, tmpbuf)
					ibuf = OtpInputStream(tmpbuf, flags)
					if (ibuf.read1() != passThrough) {
						throw new Exception("xxx")
//						break
					}

					var reason: Any = null
					var cookie: Symbol = null
					var tmp: Any = null

					var head: Product = null //TODO Any
					var toName: Symbol = null
					var to: OtpErlangPid = null
					var from: OtpErlangPid = null
					var tag = -1

					// decode the header
					tmp = ibuf.read_any();
					if (tmp.isInstanceOf[Product]) {
						val name = tmp.asInstanceOf[Product].productPrefix
						if (!name.startsWith("Tuple")) throw new Exception("xxx")
					}

					head = tmp.asInstanceOf[Product];

					if (!(head.productElement(0).isInstanceOf[Long])) {
						throw new Exception("xxx")
					}

					// lets see what kind of message this is
					tag = (head.productElement(0).asInstanceOf[Long]).asInstanceOf[Int];
					tag match {
						case (this.sendTag | this.sendTTTag) => // { SEND, Cookie, ToPid , TraceToken }					
							if (!cookieOk) {
								// we only check this once, he can send us bad cookies
								// later if he likes  ErlangAtom <=> scala Symbol
								if (!(head.productElement(1).isInstanceOf[Symbol])) {
//									break
									throw new Exception("xxx")
								}
								cookie = head.productElement(1).asInstanceOf[Symbol]
								if (sendCookie) {
									if (!cookie.name.equals(self.cookies())) {
										cookieError(self, cookie)
									}
								} else {
									if (!cookie.name.equals("")) {
										cookieError(self, cookie)
									}
								}
								cookieOk = true
							}

							if (traceLevel >= sendThreshold) {
								System.out.println("<- " + headerType(head) + " " + head)

								/* show received payload too */
								ibuf.mark(0)
								traceobj = ibuf.read_any()

								if (traceobj != null) {
									System.out.println("   " + traceobj)
								} else {
									System.out.println("   (null)")
								}
								ibuf.reset()
							}
							to = head.productElement(2).asInstanceOf[OtpErlangPid]
							deliver(OtpMsg(to, ibuf))
						case (this.regSendTag | this.regSendTTTag) => // { REG_SEND, FromPid, Cookie, ToName TraceToken }
							if (!cookieOk) {
								// we only check this once, he can send us bad cookies
								// later if he likes
								if (!(head.productElement(2).isInstanceOf[Symbol])) {
									throw new Exception("xxx")
//									break
								}
								cookie = head.productElement(2).asInstanceOf[Symbol]
								if (sendCookie) {
									if (!cookie.name.equals(self.cookies())) {
										cookieError(self, cookie)
									}
								} else {
									if (!cookie.name.equals("")) {
										cookieError(self, cookie)
									}
								}
								cookieOk = true
							}

							if (traceLevel >= sendThreshold) {
								System.out.println("<- " + headerType(head) + " "+ head)
								/* show received payload too */
								ibuf.mark(0)
								traceobj = ibuf.read_any()

								if (traceobj != null) {
									System.out.println("   " + traceobj)
								} else {
									System.out.println("   (null)")
								}
								ibuf.reset()
							}

							from = head.productElement(1).asInstanceOf[OtpErlangPid]
							toName = head.productElement(3).asInstanceOf[Symbol]
//							println(OtpMsg(from, toName.name, ibuf))
							deliver(OtpMsg(from, toName.name, ibuf))
						case (this.exitTag | this.exit2Tag) => // { EXIT2 EXIT, FromPid, ToPid, Reason }
							if (head.productElement(3) == null) {
								throw new Exception("xxx")
//								break
							}
							if (traceLevel >= ctrlThreshold) {
								System.out.println("<- " + headerType(head) + " "+ head)
							}
							from = head.productElement(1).asInstanceOf[OtpErlangPid]
							to = head.productElement(2).asInstanceOf[OtpErlangPid]
							reason = head.productElement(3)
							deliver(OtpMsg(tag, from, to, reason))
						case (this.exitTTTag | this.exit2TTTag) => // { EXIT2 EXIT, FromPid, ToPid, TraceToken, Reason }
							// as above, but bifferent element number
							if (head.productElement(4) == null) {
//								break
								throw new Exception("xxx")
							}
							if (traceLevel >= ctrlThreshold) {
								System.out.println("<- " + headerType(head) + " "+ head)
							}
							from = head.productElement(1).asInstanceOf[OtpErlangPid]
							to = head.productElement(2).asInstanceOf[OtpErlangPid]
							reason = head.productElement(4)
							deliver(OtpMsg(tag, from, to, reason))
						case (this.linkTag | this.unlinkTag) => // {UNLINK LINK, FromPid, ToPid}
							if (traceLevel >= ctrlThreshold) {
								System.out.println("<- " + headerType(head) + " "+ head)
							}
							from = head.productElement(1).asInstanceOf[OtpErlangPid]
							to = head.productElement(2).asInstanceOf[OtpErlangPid]
							deliver(OtpMsg(tag, from, to));
						case this.groupLeaderTag => // { GROUPLEADER, FromPid, ToPid}
							// (just show trace)
							if (traceLevel >= ctrlThreshold) {
								System.out.println("<- " + headerType(head) + " " + head)
							}

						case _ =>
							// garbage?
							throw new Exception("xxx")
					}
				} // end receive_loop
//			}
			// this section reachable only with break
			// we have received garbage from peer
			deliver(new Exception("Remote is sending garbage"));

		} // try
		catch {
			case e: Exception =>
				print(e.getMessage())
				deliver(e);
		} finally {
			close();
		}
	}

	/**
	 * <p>
	 * Set the trace level for this connection. Normally tracing is off by
	 * default unless System property OtpConnection.trace was set.
	 * </p>
	 *
	 * <p>
	 * The following levels are valid: 0 turns off tracing completely, 1 shows
	 * ordinary send and receive messages, 2 shows control messages such as link
	 * and unlink, 3 shows handshaking at connection setup, and 4 shows
	 * communication with Epmd. Each level includes the information shown by the
	 * lower ones.
	 * </p>
	 *
	 * @param level
	 *            the level to set.
	 *
	 * @return the previous trace level.
	 */
	def setTraceLevel(level: Int): Int = {
		val oldLevel = traceLevel;
		var tmp = level
		// pin the value
		if (tmp < 0) {
			tmp = 0;
		} else if (tmp > 4) {
			tmp = 4;
		}
		traceLevel = tmp;
		oldLevel
	}

	/**
	 * Get the trace level for this connection.
	 *
	 * @return the current trace level.
	 */
	def getTraceLevel(): Int = {
		traceLevel;
	}

	/**
	 * Close the connection to the remote node.
	 */
	def close() {
		done = true;
		connected = false;
		synchronized {
			try {
				if (socket != null) {
					if (traceLevel >= ctrlThreshold) {
						System.out.println("-> CLOSE");
					}
					socket.close();
				}
			} catch { /* ignore socket close errors */
				case e: IOException =>
			} finally {
				socket = null;
			}
		}
	}

	override protected def finalize() {
		close();
	}

	/**
	 * Determine if the connection is still alive. Note that this method only
	 * reports the status of the connection, and that it is possible that there
	 * are unread messages waiting in the receive queue.
	 *
	 * @return true if the connection is alive.
	 */
	def isConnected(): Boolean = {
		connected
	}

	// used by send and send_reg (message types with payload)
	protected def do_send(header: OtpOutputStream, payload: OtpOutputStream)/*: Unit = synchronized*/ {
		try {
			lock.lock();
			if (traceLevel >= sendThreshold) {
				// Need to decode header and output buffer to show trace
				// message!
				// First make OtpInputStream, then decode.
				try {
					val h = header.getOtpInputStream(5).read_any();
					System.out.println("-> " + headerType(h) + " " + h);

					var o = payload.getOtpInputStream(0).read_any();
					System.out.println("   " + o);
					o = null;
				} catch {
					case e: Exception =>
						System.out.println("   " + "can't decode output buffer:" + e);
				}
			}

			header.writeTo(socket.getOutputStream());
			payload.writeTo(socket.getOutputStream());
		} catch {
			case e: IOException =>
				close();
				throw e;
		}finally{
			lock.unlock();
		}
	}

	// used by the other message types
	protected def do_send(header: OtpOutputStream) /*: Unit = synchronized */{
		try {
			lock.lock()
			if (traceLevel >= ctrlThreshold) {
				try {
					val h = header.getOtpInputStream(5).read_any();
					System.out.println("-> " + headerType(h) + " " + h);
				} catch {
					case e: Exception =>
						System.out.println("   " + "can't decode output buffer: " + e);
				}
			}
			header.writeTo(socket.getOutputStream());
		} catch {
			case e: IOException =>
				close();
				throw e;
		}finally{
			lock.unlock()
		}
	}

	protected def headerType(h: Any): String = {
		var tag = -1;

		if (h.isInstanceOf[Product]) {
			tag = h.asInstanceOf[Product].productElement(0).asInstanceOf[Int]
		}
		//	if (h instanceof OtpErlangTuple) {
		//	    tag = (int) ((OtpErlangLong) ((OtpErlangTuple) h).elementAt(0))
		//		    .longValue();
		//	}
		tag match {
			case this.linkTag =>
				return "LINK";

			case this.sendTag =>
				return "SEND";

			case this.exitTag =>
				return "EXIT";

			case this.unlinkTag =>
				return "UNLINK";

			case this.regSendTag =>
				return "REG_SEND";

			case this.groupLeaderTag =>
				return "GROUP_LEADER";

			case this.exit2Tag =>
				return "EXIT2";

			case this.sendTTTag =>
				return "SEND_TT";

			case this.exitTTTag =>
				return "EXIT_TT";

			case this.regSendTTTag =>
				return "REG_SEND_TT";

			case this.exit2TTTag =>
				return "EXIT2_TT";
			case _ =>
				return "(unknown type)";
		}

	}

	/* this method now throws exception if we don't get full read */
	protected def readSock(s: Socket, b: Array[Byte]): Int = {
		var got = 0;
		val len = b.length;
		var i = 0;
		var is: InputStream = null;

		try{
			lock.lock()
			if (s == null) {
				throw new IOException("expected " + len+ " bytes, socket was closed");
			}
			is = s.getInputStream();
		}finally{
			lock.unlock();
		}
//		synchronized {
//		}

		while (got < len) {
			i = is.read(b, got, len - got);
			if (i < 0) {
				throw new IOException("expected " + len+ " bytes, got EOF after " + got + " bytes");
			} else if (i == 0 && len != 0) {
				throw new IOException("Remote connection closed");
			} else {
				got += i;
			}
		}
		got
	}

	protected def doAccept() {
		//与erlang的握手
		try {
			sendStatus("ok")
			val our_challenge = genChallenge()
			sendChallenge(peer.distChoose, self.flags, our_challenge)
			val her_challenge = recvChallengeReply(our_challenge)
			val our_digest = genDigest(her_challenge, self.cookies())
			sendChallengeAck(our_digest)
			connected = true
			cookieOk = true
			sendCookie = false
		} catch {
			case e: IOException =>
				close();
				throw e;
			case e: Exception =>
				close();
				throw e;
		}
		if (traceLevel >= handshakeThreshold) {
			System.out.println("<- MD5 ACCEPTED " + peer.hosts());
		}
	}

	protected def doConnect(port: Int) {
		try {
			socket = new Socket(peer.hosts(), port);
			socket.setTcpNoDelay(true);

			if (traceLevel >= handshakeThreshold) {
				System.out.println("-> MD5 CONNECT TO " + peer.hosts() + ":"
					+ port);
			}
			sendName(peer.distChoose, self.flags);
			recvStatus();
			val her_challenge = recvChallenge();
			val our_digest = genDigest(her_challenge, self.cookies());
			val our_challenge = genChallenge();
			sendChallengeReply(our_challenge, our_digest);
			recvChallengeAck(our_challenge);
			cookieOk = true;
			sendCookie = false;
		} catch {
			case ae: Exception =>
				close();
				throw ae;
		}
	}

	// This is nooo good as a challenge,
	// XXX fix me.
	protected def genChallenge(): Int = {
		random.nextInt(Int.MaxValue);
	}

	// Used to debug print a message digest
	def hex0(x: Byte): String = {
		val tab = Array.apply('0', '1', '2', '3', '4', '5', '6', '7', '8', '9','a', 'b', 'c', 'd', 'e', 'f');
		var uint = 0;
		if (x < 0) {
			uint = x & 0x7F;
			uint |= 1 << 7;
		} else {
			uint = x;
		}
		return "" + tab(uint >>> 4) + tab(uint & 0xF)
	}

	def hex(b: Array[Byte]): String = {
		val sb = new StringBuffer();
		try {
			for (i <- 0 until b.length) {
				sb.append(hex0(b(i)));
			}
		} catch {
			case e: Exception =>
			// Debug function, ignore errors.
		}
		return sb.toString();
	}

	protected def genDigest(challenge: Int, cookie: String): Array[Byte] = {
		var i = 0;
		var ch2 = 0L

		if (challenge < 0) {
			ch2 = 1L << 31;
			ch2 |= challenge & 0x7FFFFFFF;
		} else {
			ch2 = challenge;
		}
		val context = OtpMD5(0)
		context.update(cookie);
		context.update("" + ch2);

		val tmp = context.final_bytes();
		val res = new Array[Byte](tmp.length);
		for (i <- 0 until tmp.length) {
			res(i) = (tmp(i) & 0xFF).asInstanceOf[Byte]
		}
		return res;
	}

	protected def sendName(dist: Int, flags: Int) {

		val obuf = OtpOutputStream();
		val str = self.nodes();
		obuf.write2BE(str.length() + 7); // 7 bytes + nodename
		obuf.write1(AbstractNode.NTYPE_R6);
		obuf.write2BE(dist);
		obuf.write4BE(flags);
		obuf.write(str.getBytes());

		obuf.writeTo(socket.getOutputStream());

		if (traceLevel >= handshakeThreshold) {
			System.out.println("-> " + "HANDSHAKE sendName" + " flags=" + flags
				+ " dist=" + dist + " local=" + self);
		}
	}

	protected def sendChallenge(dist: Int, flags: Int, challenge: Int) {

		val obuf = OtpOutputStream(0);
		val str = self.nodes();
		obuf.write2BE(str.length() + 11); // 11 bytes + nodename
		obuf.write1(AbstractNode.NTYPE_R6);
		obuf.write2BE(dist);
		obuf.write4BE(flags);
		obuf.write4BE(challenge);
		obuf.write(str.getBytes());

		obuf.writeTo(socket.getOutputStream());

		if (traceLevel >= handshakeThreshold) {
			System.out.println("-> " + "HANDSHAKE sendChallenge" + " flags="
				+ flags + " dist=" + dist + " challenge=" + challenge
				+ " local=" + self);
		}
	}

	protected def read2BytePackage(): Array[Byte] = {

		val lbuf = new Array[Byte](2);
		var tmpbuf: Array[Byte] = null;

		readSock(socket, lbuf);
		val ibuf = OtpInputStream(lbuf, 0);
		val len = ibuf.read2BE();
		tmpbuf = new Array[Byte](len);
		readSock(socket, tmpbuf);
		return tmpbuf;
	}

	protected def recvName(peer: OtpPeer) {

		var hisname = "";

		try {
			val tmpbuf = read2BytePackage();
			val ibuf = OtpInputStream(tmpbuf, 0);
			var tmpname: Array[Byte] = null;
			val len = tmpbuf.length;
			peer.ntype = ibuf.read1();
			if (peer.ntype != AbstractNode.NTYPE_R6) {
				throw new IOException("Unknown remote node type");
			}
			peer.distLow = ibuf.read2BE()
			peer.distHigh = peer.distLow
			if (peer.distLow < 5) {
				throw new IOException("Unknown remote node type");
			}
			peer.flags = ibuf.read4BE();
			tmpname = new Array[Byte](len - 7);
			ibuf.readN(tmpname);
			hisname = OtpErlangString.newString(tmpname);
			// Set the old nodetype parameter to indicate hidden/normal status
			// When the old handshake is removed, the ntype should also be.
			if ((peer.flags & AbstractNode.dFlagPublished) != 0) {
				peer.ntype = AbstractNode.NTYPE_R4_ERLANG;
			} else {
				peer.ntype = AbstractNode.NTYPE_R4_HIDDEN;
			}

			if ((peer.flags & AbstractNode.dFlagExtendedReferences) == 0) {
				throw new IOException(
					"Handshake failed - peer cannot handle extended references");
			}

			if ((peer.flags & AbstractNode.dFlagExtendedPidsPorts) == 0) {
				throw new IOException(
					"Handshake failed - peer cannot handle extended pids and ports");
			}

		} catch {
			case e: Exception =>
				throw new IOException("Handshake failed - not enough data");
		}

		val i = hisname.indexOf('@', 0);
		peer.node = hisname;
		peer.alive = hisname.substring(0, i);
		peer.host = hisname.substring(i + 1, hisname.length());

		if (traceLevel >= handshakeThreshold) {
			System.out.println("<- " + "HANDSHAKE" + " ntype=" + peer.ntype
				+ " dist=" + peer.distHigh + " remote=" + peer);
		}
	}

	protected def recvChallenge(): Int = {

		var challenge = 0;

		try {
			val buf = read2BytePackage();
			val ibuf = OtpInputStream(buf, 0);
			peer.ntype = ibuf.read1();
			if (peer.ntype != AbstractNode.NTYPE_R6) {
				throw new IOException("Unexpected peer type");
			}
			peer.distLow = ibuf.read2BE();
			peer.distHigh = peer.distLow
			peer.flags = ibuf.read4BE();
			challenge = ibuf.read4BE();
			val tmpname = new Array[Byte](buf.length - 11);
			ibuf.readN(tmpname);
			val hisname = OtpErlangString.newString(tmpname);
			if (!hisname.equals(peer.node)) {
				throw new IOException(
					"Handshake failed - peer has wrong name: " + hisname);
			}

			if ((peer.flags & AbstractNode.dFlagExtendedReferences) == 0) {
				throw new IOException(
					"Handshake failed - peer cannot handle extended references");
			}

			if ((peer.flags & AbstractNode.dFlagExtendedPidsPorts) == 0) {
				throw new IOException(
					"Handshake failed - peer cannot handle extended pids and ports");
			}

		} catch {
			case e: Exception =>
				throw new IOException("Handshake failed - not enough data");
		}

		if (traceLevel >= handshakeThreshold) {
			System.out.println("<- " + "HANDSHAKE recvChallenge" + " from="
				+ peer.node + " challenge=" + challenge + " local=" + self);
		}

		challenge
	}

	protected def sendChallengeReply(challenge: Int, digest: Array[Byte]) {

		val obuf = OtpOutputStream(0);
		obuf.write2BE(21);
		obuf.write1(ChallengeReply);
		obuf.write4BE(challenge);
		obuf.write(digest);
		obuf.writeTo(socket.getOutputStream());

		if (traceLevel >= handshakeThreshold) {
			System.out.println("-> " + "HANDSHAKE sendChallengeReply"
				+ " challenge=" + challenge + " digest=" + hex(digest)
				+ " local=" + self);
		}
	}

	// 比较2数组元素值是否相等
	private def digests_equals(a: Array[Byte], b: Array[Byte]): Boolean = {
		for (i <- 0 until 16) {
			if (a(i) != b(i)) {
				return false;
			}
		}
		return true;
	}

	protected def recvChallengeReply(our_challenge: Int): Int = {

		var challenge = -1;
		val her_digest = new Array[Byte](16)

		try {
			val buf = read2BytePackage();
			val ibuf = OtpInputStream(buf, 0);
			val tag = ibuf.read1();
			if (tag != ChallengeReply) {
				throw new IOException("Handshake protocol error");
			}
			challenge = ibuf.read4BE();
			ibuf.readN(her_digest);
			val our_digest = genDigest(our_challenge, self.cookies());
			if (!digests_equals(her_digest, our_digest)) {
				throw new Exception("Peer authentication error.");
			}
		} catch {
			case e: IOException =>
				throw new IOException("Handshake failed - not enough data");
		}

		if (traceLevel >= handshakeThreshold) {
			System.out.println("<- " + "HANDSHAKE recvChallengeReply"
				+ " from=" + peer.node + " challenge=" + challenge
				+ " digest=" + hex(her_digest) + " local=" + self);
		}
		challenge
	}

	protected def sendChallengeAck(digest: Array[Byte]) {

		val obuf = OtpOutputStream(0);
		obuf.write2BE(17);
		obuf.write1(ChallengeAck);
		obuf.write(digest);

		obuf.writeTo(socket.getOutputStream());

		if (traceLevel >= handshakeThreshold) {
			System.out.println("-> " + "HANDSHAKE sendChallengeAck"
				+ " digest=" + hex(digest) + " local=" + self);
		}
	}

	protected def recvChallengeAck(our_challenge: Int) {

		val her_digest = new Array[Byte](16);
		try {
			val buf = read2BytePackage();
			val ibuf = OtpInputStream(buf, 0);
			val tag = ibuf.read1();
			if (tag != ChallengeAck) {
				throw new IOException("Handshake protocol error");
			}
			ibuf.readN(her_digest);
			val our_digest = genDigest(our_challenge, self.cookies());
			if (!digests_equals(her_digest, our_digest)) {
				throw new Exception("Peer authentication error.");
			}
		} catch {
			case e: Exception =>
				throw new IOException("Handshake failed - not enough data or auth");
		}

		if (traceLevel >= handshakeThreshold) {
			System.out.println("<- " + "HANDSHAKE recvChallengeAck" + " from="
				+ peer.node + " digest=" + hex(her_digest) + " local="
				+ self);
		}
	}

	protected def sendStatus(status: String) {

		val obuf = OtpOutputStream(0);
		obuf.write2BE(status.length() + 1);
		obuf.write1(ChallengeStatus);
		obuf.write(status.getBytes());

		obuf.writeTo(socket.getOutputStream());

		if (traceLevel >= handshakeThreshold) {
			System.out.println("-> " + "HANDSHAKE sendStatus" + " status="
				+ status + " local=" + self);
		}
	}

	protected def recvStatus() {

		try {
			val buf = read2BytePackage();
			val ibuf = OtpInputStream(buf, 0);
			val tag = ibuf.read1();
			if (tag != ChallengeStatus) {
				throw new IOException("Handshake protocol error");
			}
			val tmpbuf = new Array[Byte](buf.length - 1);
			ibuf.readN(tmpbuf);
			val status = OtpErlangString.newString(tmpbuf);

			if (status.compareTo("ok") != 0) {
				throw new IOException("Peer replied with status '" + status
					+ "' instead of 'ok'");
			}
		} catch {
			case e: Exception =>
				throw new IOException("Handshake failed - not enough data");
		}
		if (traceLevel >= handshakeThreshold) {
			System.out.println("<- " + "HANDSHAKE recvStatus (ok)" + " local="
				+ self);
		}
	}

	def setFlags(flags: Int) {
		this.flags = flags;
	}

	def getFlags(): Int = {
		return flags;
	}
}