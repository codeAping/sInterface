package com.qifun.otp.erlang

import com.qifun.otp.erlang._
import java.net.Socket
import java.io.IOException
import java.net.InetAddress
import scala.util.control.Breaks._
import java.io.ByteArrayOutputStream

/**
 * Erlang Port Mapper Daemon
 * 
 * 		负责与Erlang节点间通信
 */
object OtpEpmd {

	// common values
	var stopReq = (115).asInstanceOf[Byte]
	var port4req = (122).asInstanceOf[Byte]
	var port4resp = (119).asInstanceOf[Byte]
	var publish4req = (120).asInstanceOf[Byte]
	var publish4resp = (121).asInstanceOf[Byte]
	var names4req = (110).asInstanceOf[Byte]

	var traceLevel = 0
	val traceThreshold = 4;
	{
		var trace = System.getProperties().getProperty("OtpConnection.trace")
		try {
			if (trace != null) {
				traceLevel = Integer.valueOf(trace).intValue()
			}
		} catch {
			case e: NumberFormatException => traceLevel = 0
		}
	}

	def useEpmdPort(port: Int) {
		EpmdPort.set(port)
	}

	def lookupPort(node: AbstractNode): Int = {
		r4_lookupPort(node)
	}

	def publishPort(node: OtpLocalNode): Boolean = {
		var s: Socket = null
		s = r4_publish(node)
		node.setEpmd(s)
		s != null
	}

	def unPublishPort(node: OtpLocalNode) {
		var s: Socket = null
		val host: String = null
		try {
			s = new Socket(host, EpmdPort.get())
			val obuf = new OtpOutputStream()
			obuf.write2BE(node.alives().length() + 1)
			obuf.write1(stopReq)
			obuf.writeN(node.alives().getBytes())
			obuf.writeTo(s.getOutputStream())
			// don't even wait for a response (is there one?)
			if (traceLevel >= traceThreshold) {
				System.out.println("-> UNPUBLISH " + node + " port=" + node.ports())
				System.out.println("<- OK (assumed)")
			}
		} catch { /* ignore all failures */
			case e: Exception =>
		} finally {
			try {
				if (s != null) {
					s.close()
				}
			} catch { /* ignore close failure */
				case e: Exception =>
			}
			s = null
		}
	}

	/**
	 *
	 */
	def r4_lookupPort(node: AbstractNode): Int = {
		var port = 0
		var s: Socket = null
		try {
			val obuf = new OtpOutputStream()
			s = new Socket(node.hosts(), EpmdPort.get())

			// build and send epmd request
			// length[2], tag[1], alivename[n] (length = n+1)
			obuf.write2BE(node.alives().length() + 1)
			obuf.write1(port4req)
			obuf.writeN(node.alives().getBytes())

			// send request
			obuf.writeTo(s.getOutputStream())

			if (traceLevel >= traceThreshold) {
				System.out.println("-> LOOKUP (r4) " + node)
			}

			val tmpbuf = new Array[Byte](100)

			val n = s.getInputStream().read(tmpbuf)

			if (n < 0) {
				s.close();
				throw new IOException("Nameserver not responding on " + node.hosts() + " when looking up " + node.alives())
			}

			val ibuf = OtpInputStream(tmpbuf, 0)

			val response = ibuf.read1()
			if (response == port4resp) {
				val result = ibuf.read1()
				if (result == 0) {
					port = ibuf.read2BE()

					node.ntype = ibuf.read1()
					node.proto = ibuf.read1()
					node.distHigh = ibuf.read2BE()
					node.distLow = ibuf.read2BE()
					// ignore rest of fields
				}
			}
		} catch {
			case e: IOException =>
				if (traceLevel >= traceThreshold) {
					System.out.println("<- (no response)")
				}
				throw new IOException("Nameserver not responding on " + node.hosts() + " when looking up " + node.alives())

			case e: Exception =>
				if (traceLevel >= traceThreshold) {
					System.out.println("<- (invalid response)")
				}
				throw new IOException("Nameserver not responding on " + node.hosts() + " when looking up " + node.alives())
		} finally {
			try {
				if (s != null) {
					s.close()
				}
			} catch { /* ignore close errors */
				case e: IOException =>
			}
			s = null
		}

		if (traceLevel >= traceThreshold) {
			if (port == 0) {
				System.out.println("<- NOT FOUND")
			} else {
				System.out.println("<- PORT " + port)
			}
		}
		port
	}

	/*
     * this function will get an exception if it tries to talk to a
     * very old epmd, or if something else happens that it cannot
     * forsee. In both cases we return an exception. We no longer
     * support r3, so the exception is fatal. If we manage to
     * successfully communicate with an r4 epmd, we return either the
     * socket, or null, depending on the result.
     */
	def r4_publish(node: OtpLocalNode): Socket = {
		var s: Socket = null
		val host: String = null
		try {
			val obuf = new OtpOutputStream()
			s = new Socket(host, EpmdPort.get())

			obuf.write2BE(node.alives().length() + 13)

			obuf.write1(publish4req)
			obuf.write2BE(node.ports())

			obuf.write1(node.types())

			obuf.write1(node.protos())
			obuf.write2BE(node.distHighs())
			obuf.write2BE(node.distLows())

			obuf.write2BE(node.alives().length())
			obuf.writeN(node.alives().getBytes())
			obuf.write2BE(0) // No extra

			// send request
			obuf.writeTo(s.getOutputStream())

			if (traceLevel >= traceThreshold) {
				System.out.println("-> PUBLISH (r4) " + node + " port=" + node.ports())
			}

			// get reply
			val tmpbuf = new Array[Byte](100)
			val n = s.getInputStream().read(tmpbuf)

			if (n < 0) {
				if (s != null) {
					s.close()
				}
				throw new IOException("Nameserver not responding on " + node.hosts() + " when publishing " + node.alives())
			}

			val ibuf = OtpInputStream(tmpbuf, 0)

			val response = ibuf.read1()
			if (response == publish4resp) {
				val result = ibuf.read1()
				if (result == 0) {
					node.creation = ibuf.read2BE()
					if (traceLevel >= traceThreshold) {
						System.out.println("<- OK")
					}
					return s // success
				}
			}
		} catch {
			case e: IOException =>
				// epmd closed the connection = fail
				if (s != null) {
					s.close()
				}
				if (traceLevel >= traceThreshold) {
					System.out.println("<- (no response)")
				}
				throw new IOException("Nameserver not responding on " + node.hosts()
					+ " when publishing " + node.alives())
			case e: Exception =>
				if (s != null) {
					s.close()
				}
				if (traceLevel >= traceThreshold) {
					System.out.println("<- (invalid response)")
				}
				throw new IOException("Nameserver not responding on " + node.hosts()
					+ " when publishing " + node.alives())
		}

		if (s != null) {
			s.close()
		}
		return null
	}

	def lookupNames(): Array[String] = {
		lookupNames(InetAddress.getByName(null))
	}

	/**
	 * 查询主机名
	 */
	def lookupNames(address: InetAddress): Array[String] = {
		var s: Socket = null
		try {
			val obuf = new OtpOutputStream()
			try {
				s = new Socket(address, EpmdPort.get())

				obuf.write2BE(1)
				obuf.write1(names4req)
				// send request
				obuf.writeTo(s.getOutputStream())

				if (traceLevel >= traceThreshold) {
					System.out.println("-> NAMES (r4) ")
				}

				// get reply
				val buffer = new Array[Byte](256)
				val out = new ByteArrayOutputStream(256)
				breakable {
					while (true) {
						val bytesRead = s.getInputStream().read(buffer)
						if (bytesRead == -1) {
							break
						}
						out.write(buffer, 0, bytesRead)
					}
				}
				val tmpbuf = out.toByteArray()
				val ibuf = OtpInputStream(tmpbuf, 0)
				ibuf.read4BE() // read port int
				// final int port = ibuf.read4BE();
				// check if port = epmdPort

				val n = tmpbuf.length
				val buf = new Array[Byte](n - 4)
				System.arraycopy(tmpbuf, 4, buf, 0, n - 4)
				val all = OtpErlangString.newString(buf)
				return all.split("\n")
				//			return all.split("\n");
			} finally {
				if (s != null) {
					s.close()
				}
			}

		} catch {
			case e: IOException =>
				if (traceLevel >= traceThreshold) {
					System.out.println("<- (no response)")
				}
				throw new IOException(
					"Nameserver not responding when requesting names")
			case e: Exception =>
				if (traceLevel >= traceThreshold) {
					System.out.println("<- (invalid response)")
				}
				throw new IOException(
					"Nameserver not responding when requesting names")

		}
	}
}
object EpmdPort {
	var epmdPort: Int = 0

	/**
	 * 获取配置端口
	 */
	def get(): Int = {
		if (epmdPort == 0) {
			var env = ""
			try {
				env = System.getenv("ERL_EPMD_PORT")
			} catch {
				case e: java.lang.SecurityException => env = null
			}
			if (env != null) {
				epmdPort = Integer.parseInt(env)
			} else {
				epmdPort = 4369
			}
		}
		epmdPort
	}
	/**
	 * 设置端口
	 */
	def set(port: Int) {
		epmdPort = port
	}
}