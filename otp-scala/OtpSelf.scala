package com.qifun.otp.erlang

import java.net.ServerSocket
import java.io.IOException
import java.net.Socket

object OtpSelf {
	def apply(node: String) = {
		val self = new OtpSelf
		self._otpSelf(node)
		self
	}
	
	def apply(node: String, cookie: String) = {
		val self = new OtpSelf
		self._otpSelf(node,cookie)
		self
	}

}

class OtpSelf extends OtpLocalNode {
	private var sock: ServerSocket = null
	private var pid: OtpErlangPid = null

	def _otpSelf(node: String) {
		_otpSelf(node, defaultCookie, 0)
	}

	/**
	 * Create a self node.
	 *
	 * @param node
	 *                the name of this node.
	 *
	 * @param cookie
	 *                the authorization cookie that will be used by this node
	 *                when it communicates with other nodes.
	 */
	def _otpSelf(node: String, cookie: String) {
		_otpSelf(node, cookie, 0)
	}

	def _otpSelf(node: String, cookie: String, port: Int) {
		_otpLocalNode(node, cookie)
		sock = new ServerSocket(port)

		if (port != 0) {
			this.port = port
		} else {
			this.port = sock.getLocalPort()
		}

		pid = createPid()
	}

	/**
	 * Get the Erlang PID that will be used as the sender id in all "anonymous"
	 * messages sent by this node. Anonymous messages are those sent via send
	 * methods in {@link OtpConnection OtpConnection} that do not specify a
	 * sender.
	 *
	 * @return the Erlang PID that will be used as the sender id in all
	 *         anonymous messages sent by this node.
	 */
	def pids(): OtpErlangPid = {
		pid
	}

	/**
	 * Make public the information needed by remote nodes that may wish to
	 * connect to this one. This method establishes a connection to the Erlang
	 * port mapper (Epmd) and registers the server node's name and port so that
	 * remote nodes are able to connect.
	 *
	 * <p>
	 * This method will fail if an Epmd process is not running on the localhost.
	 * See the Erlang documentation for information about starting Epmd.
	 *
	 * <p>
	 * Note that once this method has been called, the node is expected to be
	 * available to accept incoming connections. For that reason you should make
	 * sure that you call {@link #accept()} shortly after calling
	 * {@link #publishPort()}. When you no longer intend to accept connections
	 * you should call {@link #unPublishPort()}.
	 *
	 * @return true if the operation was successful, false if the node was
	 *         already registered.
	 */
	def publishPort(): Boolean = {
		if (getEpmd() != null) {
			return false // already published
		}
		OtpEpmd.publishPort(this)
		getEpmd() != null
	}

	/**
	 * Unregister the server node's name and port number from the Erlang port
	 * mapper, thus preventing any new connections from remote nodes.
	 */
	def unPublishPort() {
		// unregister with epmd
		OtpEpmd.unPublishPort(this)
		// close the local descriptor (if we have one)
		try {
			if (epmd != null) {
				epmd.close()
			}
		} catch { /* ignore close errors */
			case e: IOException =>
		}
		epmd = null
	}

	/**
	 * Accept an incoming connection from a remote node. A call to this method
	 * will block until an incoming connection is at least attempted.
	 *
	 * @return a connection to a remote node.
	 */
	def accept(): OtpConnection = {
		var newsock: Socket = null
		while (true) {
			try {
				newsock = sock.accept()
				return OtpConnection(this, newsock)
			} catch {
				case ex: IOException =>
					try {
						if (newsock != null) {
							newsock.close()
						}
					} catch { /* ignore close errors */
						case e: IOException =>
					}
					throw ex
			}
		}
		return null
	}

	/**
	 * Open a connection to a remote node.
	 *
	 * @param other
	 *                the remote node to which you wish to connect.
	 *
	 * @return a connection to the remote node.
	 */
	@throws(classOf[IOException])
	def connect(other: OtpPeer): OtpConnection = {
		OtpConnection(this, other)
	}
}
