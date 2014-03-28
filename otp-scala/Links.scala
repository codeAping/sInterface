package com.qifun.otp.erlang

object Links {
	def apply(initialSize: Int) = {
		val links = new Links(initialSize)
		links.init
		links
	}
}

/**
 * PID 管理
 */
class Links(initialSize: Int = 10) {
	var links: Array[Link] = null
	var count = 0

	def init() {
		count = 0
		links = new Array[Link](initialSize)
	}

	def addLink(local: OtpErlangPid, remote: OtpErlangPid): Unit = synchronized {
		if (find(local, remote) == -1) {
			if (count >= links.length) {
				val tmp = new Array[Link](count * 2)
				System.arraycopy(links, 0, tmp, 0, count)
				links = tmp
			}
			links(count) = Link(local, remote)
			count += 1
		}
	}

	def removeLink(local: OtpErlangPid, remote: OtpErlangPid): Unit = synchronized {
		var i = find(local, remote)
		if (i != -1) {
			count -= 1
			links(i) = links(count)
			links(count) = null
		}
	}

	def exists(local: OtpErlangPid, remote: OtpErlangPid): Boolean = synchronized {
		find(local, remote) != -1
	}

	def find(local: OtpErlangPid, remote: OtpErlangPid): Int = synchronized {
		val size = links.length
		for (i <- 0 until size) {
			if (links(i).equals(local, remote)) {
				return i
			}
		}
		return -1
	}

	def counts(): Int = {
		count
	}

	/* all local pids get notified about broken connection */
	def localPids(): Array[OtpErlangPid] = synchronized {
		var ret: Array[OtpErlangPid] = null
		if (count != 0) {
			ret = new Array[OtpErlangPid](count)
			for (i <- 0 until count) {
				ret(i) = links(i).locals()
			}
		}
		ret
	}

	/* all remote pids get notified about failed pid */
	def remotePids(): Array[OtpErlangPid] = synchronized {
		var ret: Array[OtpErlangPid] = null
		if (count != 0) {
			ret = new Array[OtpErlangPid](count)
			for (i <- 0 until count) {
				ret(i) = links(i).remotes();
			}
		}
		ret
	}

	/* clears the link table, returns a copy */
	def clearLinks(): Array[Link] = synchronized {
		var ret: Array[Link] = null;
		if (count != 0) {
			ret = new Array[Link](count)
			for (i <- 0 until count) {
				ret(i) = links(i)
				links(i) = null
			}
			count = 0
		}
		ret
	}

	/** returns a copy of the link table */
	def getLinks(): Array[Link] = synchronized {
		var ret: Array[Link] = null
		if (count != 0) {
			ret = new Array[Link](count)
			System.arraycopy(links, 0, ret, 0, count)
		}
		ret
	}
}