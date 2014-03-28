package com.qifun.otp.erlang

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

trait ConstsNode{
	// Node types
	val NTYPE_R6 = 110; // 'n' post-r5, all nodes
	val NTYPE_R4_ERLANG = 109; // 'm' Only for source compatibility
	val NTYPE_R4_HIDDEN = 104; // 'h' Only for source compatibility

	// Node capability flags
	val dFlagPublished = 1;
	val dFlagAtomCache = 2;
	val dFlagExtendedReferences = 4;
	val dFlagDistMonitor = 8;
	val dFlagFunTags = 0x10;
	val dFlagDistMonitorName = 0x20; // NOT USED
	val dFlagHiddenAtomCache = 0x40; // NOT SUPPORTED
	val dflagNewFunTags = 0x80;
	val dFlagExtendedPidsPorts = 0x100;
	val dFlagExportPtrTag = 0x200; // NOT SUPPORTED
	val dFlagBitBinaries = 0x400;
	val dFlagNewFloats = 0x800;
	val dFlagUnicodeIo = 0x1000;
	val dFlagUtf8Atoms = 0x10000;
}


class AbstractNode(nodes: String = "", cookies: String = "") extends ConstsNode{
	var localHost: String = "";
	var node: String= "";
	var host: String= "";
	var alive: String= "";
	var cookie: String= "";
	var defaultCookie: String= "";

//	// Node types
//	val NTYPE_R6 = 110; // 'n' post-r5, all nodes
//	val NTYPE_R4_ERLANG = 109; // 'm' Only for source compatibility
//	val NTYPE_R4_HIDDEN = 104; // 'h' Only for source compatibility
//
//	// Node capability flags
//	val dFlagPublished = 1;
//	val dFlagAtomCache = 2;
//	val dFlagExtendedReferences = 4;
//	val dFlagDistMonitor = 8;
//	val dFlagFunTags = 0x10;
//	val dFlagDistMonitorName = 0x20; // NOT USED
//	val dFlagHiddenAtomCache = 0x40; // NOT SUPPORTED
//	val dflagNewFunTags = 0x80;
//	val dFlagExtendedPidsPorts = 0x100;
//	val dFlagExportPtrTag = 0x200; // NOT SUPPORTED
//	val dFlagBitBinaries = 0x400;
//	val dFlagNewFloats = 0x800;
//	val dFlagUnicodeIo = 0x1000;
//	val dFlagUtf8Atoms = 0x10000;

	var ntype = NTYPE_R6;
	var proto = 0; // tcp/ip
	var distHigh = 5; // Cannot talk to nodes before R6
	var distLow = 5; // Cannot talk to nodes before R6
	var creation = 0;
	var flags = dFlagExtendedReferences | dFlagExtendedPidsPorts | dFlagBitBinaries | dFlagNewFloats | dFlagFunTags | dflagNewFunTags | dFlagUtf8Atoms;

	/* initialize hostname and default cookie */

	try {
		localHost = java.net.InetAddress.getLocalHost().getHostName();
		val dot = localHost.indexOf(".");
		if (dot != -1) {
			localHost = localHost.substring(0, dot);
		}
	} catch {
		case e: UnknownHostException => localHost = "localhost";
	}

	val homeDir = getHomeDir();
	val dotCookieFilename = homeDir + File.separator + ".erlang.cookie";
	var br: BufferedReader = null;

	try {
		var dotCookieFile = new File(dotCookieFilename);
		br = new BufferedReader(new FileReader(dotCookieFile));
		defaultCookie = br.readLine().trim();
	} catch {
		case e: IOException => defaultCookie = "";
	} finally {
		try {
			if (br != null) {
				br.close();
			}
		} catch {
			case e: Exception => println("close stream error~!")
		}
	}

	//    protected AbstractNode() {
	//    }

	/**
	 * Create a node with the given name and the default cookie.
	 */
	def abstractNode(node: String) {
		abstractNode(node, defaultCookie);
	}

	/**
	 * Create a node with the given name and cookie.
	 */
	def abstractNode(name: String, cookie: String) {
		this.cookie = cookie;

		val i = name.indexOf('@', 0);
		if (i < 0) {
			alive = name;
			host = localHost;
		} else {
			alive = name.substring(0, i);
			host = name.substring(i + 1, name.length());
		}

		if (alive.length() > 0xff) {
			alive = alive.substring(0, 0xff);
		}

		node = alive + "@" + host;
	}

	/**
	 * Get the name of this node.
	 *
	 * @return the name of the node represented by this object.
	 */
	def nodes(): String = {
		node;
	}

	/**
	 * Get the hostname part of the nodename. Nodenames are composed of two
	 * parts, an alivename and a hostname, separated by '@'. This method returns
	 * the part of the nodename following the '@'.
	 *
	 * @return the hostname component of the nodename.
	 */
	def hosts(): String = {
		host;
	}

	/**
	 * Get the alivename part of the hostname. Nodenames are composed of two
	 * parts, an alivename and a hostname, separated by '@'. This method returns
	 * the part of the nodename preceding the '@'.
	 *
	 * @return the alivename component of the nodename.
	 */
	def alives(): String = {
		alive;
	}

	/**
	 * Get the authorization cookie used by this node.
	 *
	 * @return the authorization cookie used by this node.
	 */
	def cookies(): String = {
		cookie;
	}

	// package scope
	def types(): Int = {
		ntype;
	}

	// package scope
	def distHighs(): Int = {
		distHigh;
	}

	// package scope
	def distLows(): Int = {
		distLow;
	}

	// package scope: useless information?
	def protos(): Int = {
		proto;
	}

	// package scope
	def creations(): Int = {
		creation;
	}

	/**
	 * Set the authorization cookie used by this node.
	 *
	 * @return the previous authorization cookie used by this node.
	 */
	def setCookie(cookie: String): String = {
		val prev = this.cookie;
		this.cookie = cookie;
		return prev;
	}

	override def toString(): String = {
		return nodes();
	}

	def getHomeDir(): String = {
		val home = System.getProperty("user.home");
		if (System.getProperty("os.name").toLowerCase().contains("windows")) {
			val drive = System.getenv("HOMEDRIVE");
			val path = System.getenv("HOMEPATH");
			if (drive != null && path != null) return drive + path
			else return home
		} else {
			return home;
		}
	}
}


object AbstractNode extends ConstsNode{
	def apply(nodes:String = "",cookies : String = "") = {
		new AbstractNode(nodes,cookies)
	}
}