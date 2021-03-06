package com.qifun.otp.erlang

import java.io.ByteArrayInputStream
import java.io.IOException
import java.math.BigDecimal
import java.io.UnsupportedEncodingException
import scala.collection.mutable.WrappedArray

/**
 * scala <-----> erlang 解码模块
 *
 * 提供了读取各种数据类型接口
 */
class OtpInputStream(arr: Array[Byte]) extends ByteArrayInputStream(arr: Array[Byte]) {

	val DECODE_INT_LISTS_AS_STRINGS = 1

	private var flags = 0

	//    /**
	//     * @param buf
	//     */
	def _otpInputStream(bufs: Array[Byte]) {
		_otpInputStream(bufs, 0)
	}
	//
	//    /**
	//     * Create a stream from a buffer containing encoded Erlang terms.
	//     * 
	//     * @param flags
	//     */
	def _otpInputStream(bufs: Array[Byte], flags: Int) {
		buf = bufs
		pos = 0
		count = bufs.length
		this.flags = flags
	}

	//    /**
	//     * Create a stream from a buffer containing encoded Erlang terms at the
	//     * given offset and length.
	//     * 
	//     * @param flags
	//     */
	def _otpInputStream(bufs: Array[Byte], offset: Int, length: Int, flags: Int) {
		this.buf = bufs;
		this.pos = offset;
		this.count = Math.min(offset + length, buf.length);
		this.mark = offset;
		this.flags = flags;
	}

	/**
	 * Get the current position in the stream.
	 *
	 * @return the current position in the stream.
	 */
	def getPos(): Int = {
		pos
	}

	/**
	 * Set the current position in the stream.
	 *
	 * @param pos
	 *            the position to move to in the stream. If pos indicates a
	 *            position beyond the end of the stream, the position is move to
	 *            the end of the stream instead. If pos is negative, the
	 *            position is moved to the beginning of the stream instead.
	 *
	 * @return the previous position in the stream.
	 */
	def setPos(posx: Int): Int = {
		val oldpos = posx;
		var tmp = posx
		if (tmp > count) {
			tmp = count
		} else if (tmp < 0) {
			tmp = 0
		}
		pos = tmp;
		oldpos;
	}

	/**
	 * Read an array of bytes from the stream. The method reads at most
	 * buf.length bytes from the input stream.
	 *
	 * @return the number of bytes read.
	 */
	def readN(buf: Array[Byte]): Int = {
		return this.readN(buf, 0, buf.length)
	}

	/**
	 * Read an array of bytes from the stream. The method reads at most len
	 * bytes from the input stream into offset off of the buffer.
	 *
	 * @return the number of bytes read.
	 */
	def readN(buf: Array[Byte], off: Int, len: Int): Int = {
		if (len == 0 && available() == 0) {
			return 0
		}
		val i = super.read(buf, off, len)
		if (i < 0) {
			throw new Exception("Cannot read from input stream")
		}
		i
	}

	/**
	 * Alias for peek1()
	 */
	def peek(): Int = {
		peek1();
	}

	/**
	 * Look ahead one position in the stream without consuming the byte found
	 * there.
	 *
	 * @return the next byte in the stream, as an integer.
	 */
	def peek1(): Int = {
		var i = 0;
		i = buf(pos)
		if (i < 0) {
			i += 256
		}
		i
	}

	def peek1skip_version(): Int = {
		var i = peek1()
		if (i == OtpExternal.versionTag) {
			read1()
			i = peek1()
		}
		i
	}

	/**
	 * Read a one byte integer from the stream.
	 *
	 * @return the byte read, as an integer.
	 */
	def read1(): Int = {
		var i = super.read()

		if (i < 0) {
			throw new Exception("Cannot read from input stream")
		}
		i
	}

	def read1skip_version(): Int = {
		var tag = read1()
		if (tag == OtpExternal.versionTag) {
			tag = read1()
		}
		tag
	}

	/**
	 * Read a two byte big endian integer from the stream.
	 *
	 * @return the bytes read, converted from big endian to an integer.
	 */
	def read2BE(): Int = {
		val b = new Array[Byte](2)
		super.read(b);
		(b(0) << 8 & 0xff00) + (b(1) & 0xff)
	}

	/**
	 * Read a four byte big endian integer from the stream.
	 *
	 * @return the bytes read, converted from big endian to an integer.
	 */
	def read4BE(): Int = {
		val b = new Array[Byte](4)
		super.read(b);
		return ((b(0) << 24 & 0xff000000) + (b(1) << 16 & 0xff0000) + (b(2) << 8 & 0xff00) + (b(3) & 0xff))
	}

	/**
	 * Read a two byte little endian integer from the stream.
	 *
	 * @return the bytes read, converted from little endian to an integer.
	 */
	def read2LE(): Int = {
		val b = new Array[Byte](2)
		super.read(b)
		return (b(1) << 8 & 0xff00) + (b(0) & 0xff)
	}

	/**
	 * Read a four byte little endian integer from the stream.
	 *
	 * @return the bytes read, converted from little endian to an integer.
	 */
	def read4LE(): Int = {
		val b = new Array[Byte](4)
		super.read(b);
		return (b(3) << 24 & 0xff000000) + (b(2) << 16 & 0xff0000) + (b(1) << 8 & 0xff00) + (b(0) & 0xff)
	}

	/**
	 * Read a little endian integer from the stream.
	 *
	 * @param n
	 *            the number of bytes to read
	 *
	 * @return the bytes read, converted from little endian to an integer.
	 */
	def readLE(n: Int): Long = {
		var tmp = n
		val b = new Array[Byte](n)
		super.read(b)
		var v = 0L
		while (tmp > 0) {
			tmp = tmp - 1
			v = v << 8 | b(tmp).asInstanceOf[Long] & 0xff
		}
		v
	}

	/**
	 * Read a bigendian integer from the stream.
	 *
	 * @param n
	 *            the number of bytes to read
	 *
	 * @return the bytes read, converted from big endian to an integer.
	 */
	def readBE(n: Int): Long = {
		val b = new Array[Byte](n)
		super.read(b)
		var v = 0L
		for (i <- 0 until n) {
			v = v << 8 | b(i).asInstanceOf[Long] & 0xff
		}
		v
	}

	/**
	 * Read an Erlang atom from the stream and interpret the value as a boolean.
	 *
	 * @return true if the atom at the current position in the stream contains
	 *         the value 'true' (ignoring case), false otherwise.
	 */
	def read_boolean(): Boolean = {
		if ("true".equals(read_atom()))
			true
		else
			false
	}

	/**
	 * Read an Erlang atom from the stream.
	 *
	 * @return a String containing the value of the atom.
	 */
	def read_atom(): Symbol = {
		var tag = read1skip_version();

		tag match {
			case OtpExternal.atomTag =>
				var len = read2BE()
				var strbuf = new Array[Byte](len)
				this.readN(strbuf)
				var atom = OtpErlangString.newString(strbuf)

				if (atom.length() > OtpExternal.maxAtomLength) {
					atom = atom.substring(0, OtpExternal.maxAtomLength)
				}
				Symbol(atom)
			case OtpExternal.smallAtomUtf8Tag | OtpExternal.atomUtf8Tag =>
				var len = -1
				if (tag == OtpExternal.smallAtomUtf8Tag) {
					len = read1()
				}
				if (len < 0) {
					len = read2BE();
				}
				var strbuf = new Array[Byte](len)
				this.readN(strbuf)
				var atom = new String(strbuf, "UTF-8")
				if (atom.codePointCount(0, atom.length()) > OtpExternal.maxAtomLength) {
					var cps = OtpErlangString.stringToCodePoints(atom)
					atom = new String(cps, 0, OtpExternal.maxAtomLength)
				}
				Symbol(atom)
			case _ =>
				throw new Exception("Wrong tag encountered, expected " + tag)
		}
	}
	/**
	 * Read an Erlang binary from the stream.
	 *
	 * @return a byte array containing the value of the binary.
	 */
	def read_binary(): Array[Byte] = {

		val tag = read1skip_version()
		if (tag != OtpExternal.binTag) {
			throw new Exception(
				"Wrong tag encountered, expected " + OtpExternal.binTag + ", got " + tag);
		}

		val len = read4BE()
		val bin = new Array[Byte](len)
		this.readN(bin)
		bin
	}

	/**
	 * Read an Erlang bitstr from the stream.
	 *
	 * @param pad_bits
	 *            an int array whose first element will be set to the number of
	 *            pad bits in the last byte.
	 *
	 * @return a byte array containing the value of the bitstr.
	 */
	def read_bitstr(pad_bits: Array[Int]): Array[Byte] = {
		val tag = read1skip_version();
		if (tag != OtpExternal.bitBinTag) {
			throw new Exception(
				"Wrong tag encountered, expected " + OtpExternal.bitBinTag
					+ ", got " + tag)
		}

		val len = read4BE();
		val bin = new Array[Byte](len)
		val tail_bits = read1()
		if (tail_bits < 0 || 7 < tail_bits) {
			throw new Exception(
				"Wrong tail bit count in bitstr: " + tail_bits)
		}
		if (len == 0 && tail_bits != 0) {
			throw new Exception(
				"Length 0 on bitstr with tail bit count: " + tail_bits)
		}
		this.readN(bin)

		pad_bits(0) = (8 - tail_bits)
		bin
	}

	/**
	 * Read an Erlang float from the stream.
	 *
	 * @return the float value.
	 */
	def read_float(): Float = {
		val d = read_double()
		d.asInstanceOf[Float]
	}

	/**
	 * Read an Erlang float from the stream.
	 *
	 * @return the float value, as a double.
	 */
	def read_double(): Double = {
		// parse the stream
		val tag = read1skip_version()

		tag match {
			case OtpExternal.newFloatTag => {
				return java.lang.Double.longBitsToDouble(readBE(8))
			}
			case OtpExternal.floatTag => {
				val strbuf = new Array[Byte](31)
				// get the string
				this.readN(strbuf);
				val str = OtpErlangString.newString(strbuf)

				// find the exponent prefix 'e' in the string
				val epos = str.indexOf('e', 0)

				if (epos < 0) {
					throw new Exception("Invalid float format: '"+ str + "'")
				}

				// remove the sign from the exponent, if positive
				var estr = str.substring(epos + 1).trim()

				if (estr.substring(0, 1).equals("+")) {
					estr = estr.substring(1)
				}

				// now put the mantissa and exponent together
				val exp = Integer.valueOf(estr).intValue()
				var value = new BigDecimal(str.substring(0, epos)).movePointRight(exp)

				return value.doubleValue()
			}
			case _ =>
				throw new Exception(
					"Wrong tag encountered, expected "
						+ OtpExternal.newFloatTag + ", got " + tag)
		}
	}

	/**
	 * Read one byte from the stream.
	 *
	 * @return the byte read.
	 */
	def read_byte(): Byte = {
		val l = this.read_long(false)
		val i = l.asInstanceOf[Byte]

		if (l != i) {
			throw new Exception("Value does not fit in byte: "+ l)
		}
		i
	}

	/**
	 * Read a character from the stream.
	 *
	 * @return the character value.
	 */
	def read_char(): Char = {
		val l = this.read_long(true)
		val i = l.asInstanceOf[Char]

		if (l != (i & 0xffffL)) {
			throw new Exception("Value does not fit in char: "+ l)
		}
		i
	}

	/**
	 * Read an unsigned integer from the stream.
	 *
	 * @return the integer value.
	 */
	def read_uint(): Int = {
		val l = this.read_long(true)
		val i = l.asInstanceOf[Int]

		if (l != (i & 0xFFFFffffL)) {
			throw new Exception("Value does not fit in uint: "+ l)
		}
		i
	}

	/**
	 * Read an integer from the stream.
	 *
	 * @return the integer value.
	 */
	def read_int(): Int = {
		val l = this.read_long(false);
		val i = l.asInstanceOf[Int];

		if (l != i) {
			throw new Exception("Value does not fit in int: "+ l)
		}
		i
	}

	/**
	 * Read an unsigned short from the stream.
	 *
	 * @return the short value.
	 */
	def read_ushort(): Short = {
		val l = this.read_long(true)
		val i = l.asInstanceOf[Short]

		if (l != (i & 0xffffL)) {
			throw new Exception("Value does not fit in ushort: "+ l)
		}
		i
	}

	/**
	 * Read a short from the stream.
	 *
	 * @return the short value.
	 */
	def read_short(): Short = {
		val l = this.read_long(false)
		val i = l.asInstanceOf[Short]
		if (l != i) {
			throw new Exception("Value does not fit in short: "+ l)
		}
		i
	}

	/**
	 * Read an unsigned long from the stream.
	 *
	 * @return the long value.
	 */
	def read_ulong(): Long = {
		this.read_long(true)
	}

	/**
	 * Read a long from the stream.
	 *
	 * @return the long value.
	 */
	def read_long(): Long = {
		this.read_long(false)
	}

	def read_long(unsigned: Boolean): Long = {
		val b = read_integer_byte_array()
		byte_array_to_long(b, unsigned)
	}

	/**
	 * Read an integer from the stream.
	 *
	 * @return the value as a big endian 2's complement byte array.
	 */
	def read_integer_byte_array(): Array[Byte] = {
		var nb: Array[Byte] = null

		val tag = read1skip_version()

		tag match {
			case OtpExternal.smallIntTag =>
				nb = new Array[Byte](2)
				nb(0) = 0
				nb(1) = read1().asInstanceOf[Byte]
			case OtpExternal.intTag =>
				nb = new Array[Byte](4)
				if (this.readN(nb) != 4) { // Big endian
					throw new Exception("Cannot read from intput stream")
				}
			case (OtpExternal.largeBigTag | OtpExternal.smallBigTag) =>
				var arity = 0
				var sign = 0
				if (tag == OtpExternal.smallBigTag) {
					arity = read1()
					sign = read1()
				} else {
					arity = read4BE()
					sign = read1();
					if (arity + 1 < 0) {
						throw new Exception(
							"Value of largeBig does not fit in BigInteger, arity "
								+ arity + " sign " + sign)
					}
				}
				nb = new Array[Byte](arity + 1)
				// Value is read as little endian. The big end is augumented
				// with one zero byte to make the value 2's complement positive.
				if (this.readN(nb, 0, arity) != arity) {
					throw new Exception("Cannot read from intput stream")
				}
				// Reverse the array to make it big endian.
				var i = 0
				var j = nb.length
				while (i < j) {
					j = j - 1
					val b = nb(i)
					nb(i) = nb(j)
					nb(j) = b;
					i = i + 1
				}

				if (sign != 0) {
					// 2's complement negate the big endian value in the array
					var c = 1; // Carry
					var k = nb.length
					while (k > 0) {
						k -= 1
						c = (~nb(k) & 0xFF) + c
						nb(k) = c.asInstanceOf[Byte]
						c >>= 8
					}
				}
			case _ =>
				throw new Exception("Not valid integer tag: " + tag)
		}
		nb
	}

	def byte_array_to_long(b: Array[Byte], unsigned: Boolean): Long = {
		var v = 0L;
		b.length match {
			case 0 =>
				v = 0;
			case 2 =>
				v = ((b(0) & 0xFF) << 8) + (b(1) & 0xFF);
				v = v.asInstanceOf[Short]; // Sign extend
				if (v < 0 && unsigned) {
					throw new Exception("Value not unsigned: " + v);
				}
			case 4 =>
				v = ((b(0) & 0xFF) << 24) + ((b(1) & 0xFF) << 16) + ((b(2) & 0xFF) << 8) + (b(3) & 0xFF)
				v = v.asInstanceOf[Int]; // Sign extend
				if (v < 0 && unsigned) {
					throw new Exception("Value not unsigned: " + v)
				}
			case _ =>
				var i = 0;
				val c = b(i);
				// Skip non-essential leading bytes
				if (unsigned) {
					if (c < 0) {
						throw new Exception("Value not unsigned: "
							+ b);
					}
					while (b(i) == 0) {
						i += 1; // Skip leading zero sign bytes
					}
				} else {
					if (c == 0 || c == -1) { // Leading sign byte
						i = 1;
						// Skip all leading sign bytes
						while (i < b.length && b(i) == c) {
							i += 1;
						}
						if (i < b.length) {
							// Check first non-sign byte to see if its sign
							// matches the whole number's sign. If not one more
							// byte is needed to represent the value.
							if (((c ^ b(i)) & 0x80) != 0) {
								i -= 1;
							}
						}
					}
				}
				if (b.length - i > 8) {
					// More than 64 bits of value
					throw new Exception(
						"Value does not fit in long: " + b);
				}
				// Convert the necessary bytes
				if (c < 0) v = -1
				else v = 0
				while (i < b.length) {
					v = v << 8 | b(i) & 0xFF;
					i += 1
				}
		}
		v
	}

	/**
	 * Read a list header from the stream.
	 *
	 * @return the arity of the list.
	 */
	def read_list_head(): Int = {
		var arity = 0;
		val tag = read1skip_version();

		tag match {
			case OtpExternal.nilTag =>
				arity = 0;
			case OtpExternal.stringTag =>
				arity = read2BE();
			case OtpExternal.listTag =>
				arity = read4BE();
			case _ =>
				throw new Exception("Not valid list tag: " + tag);
		}

		arity;
	}

	/**
	 * Read a tuple header from the stream.
	 *
	 * @return the arity of the tuple.
	 */
	def read_tuple_head(): Int = {
		var arity = 0;
		val tag = read1skip_version();

		// decode the tuple header and get arity
		tag match {
			case OtpExternal.smallTupleTag =>
				arity = read1();
			case OtpExternal.largeTupleTag =>
				arity = read4BE();
			case _ =>
				throw new Exception("Not valid tuple tag: " + tag)
		}

		arity
	}

	/**
	 * Read an empty list from the stream.
	 *
	 * @return zero (the arity of the list).
	 */
	def read_nil(): Int = {
		var arity = 0
		val tag = read1skip_version()

		tag match {
			case OtpExternal.nilTag =>
				arity = 0
			case _ =>
				throw new Exception("Not valid nil tag: " + tag)
		}

		arity
	}

	/**
	 * Read an Erlang PID from the stream.
	 *
	 * @return the value of the PID.
	 */
	def read_pid(): OtpErlangPid = {
		var tag = read1skip_version()

		if (tag != OtpExternal.pidTag) {
			throw new Exception(
				"Wrong tag encountered, expected " + OtpExternal.pidTag
					+ ", got " + tag)
		}

		val node = read_atom()
		val id = read4BE() & 0x7fff // 15 bits
		val serial = read4BE() & 0x1fff // 13 bits
		val creation = read1() & 0x03 // 2 bits

		OtpErlangPid(node, id, serial, creation)
	}

	/**
	 * Read an Erlang port from the stream.
	 *
	 * @return the value of the port.
	 */
	def read_port(): OtpErlangPort = {
		val tag = read1skip_version()
		if (tag != OtpExternal.portTag) {
			throw new Exception(
				"Wrong tag encountered, expected " + OtpExternal.portTag
					+ ", got " + tag)
		}

		val node = read_atom()
		val id = read4BE() & 0xfffffff // 28 bits
		val creation = read1() & 0x03 // 2 bits

		OtpErlangPort(node, id, creation)
	}

	/**
	 * Read an Erlang reference from the stream.
	 *
	 * @return the value of the reference
	 */
	//    public OtpErlangRef read_ref() throws OtpErlangDecodeException {
	//	String node;
	//	int id;
	//	int creation;
	//	int tag;
	//
	//	tag = read1skip_version();
	//
	//	switch (tag) {
	//	case OtpExternal.refTag:
	//	    node = read_atom();
	//	    id = read4BE() & 0x3ffff; // 18 bits
	//	    creation = read1() & 0x03; // 2 bits
	//	    return new OtpErlangRef(node, id, creation);
	//
	//	case OtpExternal.newRefTag:
	//	    final int arity = read2BE();
	//	    node = read_atom();
	//	    creation = read1() & 0x03; // 2 bits
	//
	//	    final int[] ids = new int[arity];
	//	    for (int i = 0; i < arity; i++) {
	//		ids[i] = read4BE();
	//	    }
	//	    ids[0] &= 0x3ffff; // first id gets truncated to 18 bits
	//	    return new OtpErlangRef(node, ids, creation);
	//
	//	default:
	//	    throw new OtpErlangDecodeException(
	//		    "Wrong tag encountered, expected ref, got " + tag);
	//	}
	//    }
	//
	//    public OtpErlangFun read_fun() throws OtpErlangDecodeException {
	//	final int tag = read1skip_version();
	//	if (tag == OtpExternal.funTag) {
	//	    final int nFreeVars = read4BE();
	//	    final OtpErlangPid pid = read_pid();
	//	    final String module = read_atom();
	//	    final long index = read_long();
	//	    final long uniq = read_long();
	//	    final OtpErlangObject[] freeVars = new OtpErlangObject[nFreeVars];
	//	    for (int i = 0; i < nFreeVars; ++i) {
	//		freeVars[i] = read_any();
	//	    }
	//	    return new OtpErlangFun(pid, module, index, uniq, freeVars);
	//	} else if (tag == OtpExternal.newFunTag) {
	//	    final int n = read4BE();
	//	    final int arity = read1();
	//	    final byte[] md5 = new byte[16];
	//	    readN(md5);
	//	    final int index = read4BE();
	//	    final int nFreeVars = read4BE();
	//	    final String module = read_atom();
	//	    final long oldIndex = read_long();
	//	    final long uniq = read_long();
	//	    final OtpErlangPid pid = read_pid();
	//	    final OtpErlangObject[] freeVars = new OtpErlangObject[nFreeVars];
	//	    for (int i = 0; i < nFreeVars; ++i) {
	//		freeVars[i] = read_any();
	//	    }
	//	    return new OtpErlangFun(pid, module, arity, md5, index, oldIndex,
	//		    uniq, freeVars);
	//	} else {
	//	    throw new OtpErlangDecodeException(
	//		    "Wrong tag encountered, expected fun, got " + tag);
	//	}
	//    }
	//
	//    public OtpErlangExternalFun read_external_fun()
	//	    throws OtpErlangDecodeException {
	//	final int tag = read1skip_version();
	//	if (tag != OtpExternal.externalFunTag) {
	//	    throw new OtpErlangDecodeException(
	//		    "Wrong tag encountered, expected external fun, got " + tag);
	//	}
	//	final String module = read_atom();
	//	final String function = read_atom();
	//	final int arity = (int) read_long();
	//	return new OtpErlangExternalFun(module, function, arity);
	//    }

	/**
	 * Read a string from the stream.
	 *
	 * @return the value of the string.
	 */
	def read_string(): String = {
		var len = 0;
		var strbuf: Array[Byte] = null;
		var intbuf: Array[Int] = null;
		val tag = read1skip_version();
		tag match {
			case OtpExternal.stringTag =>
				len = read2BE();
				strbuf = new Array[Byte](len);
				this.readN(strbuf);
				return OtpErlangString.newString(strbuf);
			case OtpExternal.nilTag =>
				return "";
			case OtpExternal.listTag => // List when unicode +
				len = read4BE();
				intbuf = new Array[Int](len);
				for (i <- 0 until len) {
					intbuf(i) = read_int();
					if (!OtpErlangString.isValidCodePoint(intbuf(i))) {
						throw new Exception
						("Invalid CodePoint: " + intbuf(i));
					}
				}
				read_nil();
				return new String(intbuf, 0, intbuf.length);
			case _ =>
				throw new Exception(
					"Wrong tag encountered, expected " + OtpExternal.stringTag
						+ " or " + OtpExternal.listTag + ", got " + tag);
		}
	}

	/**
	 * Read a compressed term from the stream
	 *
	 * @return the resulting uncompressed term.
	 */
	//    def read_compressed() : Any = {
	//	val tag = read1skip_version();
	//
	//	if (tag != OtpExternal.compressedTag) {
	//	    throw new Exception(
	//		    "Wrong tag encountered, expected "
	//			    + OtpExternal.compressedTag + ", got " + tag);
	//	}
	//
	//	val size = read4BE();
	//	val buf = new Array[Byte](size);
	//	final java.util.zip.InflaterInputStream is = 
	//	    new java.util.zip.InflaterInputStream(this);
	//	try {
	//	    final int dsize = is.read(buf, 0, size);
	//	    if (dsize != size) {
	//		throw new OtpErlangDecodeException("Decompression gave "
	//			+ dsize + " bytes, not " + size);
	//	    }
	//	} catch (final IOException e) {
	//	    throw new OtpErlangDecodeException("Cannot read from input stream");
	//	}
	//
	//	final OtpInputStream ois = new OtpInputStream(buf, flags);
	//	return ois.read_any();
	//    }

	private def read_list(): Array[Any] = {
		val arity = read_list_head()
		if (arity > 0) {
			val elems: Array[Any] = new Array[Any](arity)
			for (i <- 0 until arity) {
				elems(i) = read_any
			}
			if (peek1 == OtpExternal.nilTag) {
				read_nil
			} else {
				val lastTail = read_any()
			}
			return elems
		}
		return new Array[Any](0)
	}

	/**
	 * Read an arbitrary Erlang term from the stream.
	 *
	 * @return the Erlang term.
	 */
	def read_any(): Any = {
		// calls one of the above functions, depending on o
		val tag = peek1skip_version();

		tag match {
			case (OtpExternal.smallIntTag | OtpExternal.intTag | OtpExternal.smallBigTag | OtpExternal.largeBigTag) =>
				val b = read_integer_byte_array();
				byte_array_to_long(b, false);

			case (OtpExternal.atomTag | OtpExternal.smallAtomUtf8Tag | OtpExternal.atomUtf8Tag) =>
				read_atom()

			case (OtpExternal.floatTag | OtpExternal.newFloatTag) =>
				read_double()

			//	case OtpExternal.refTag:
			//	case OtpExternal.newRefTag:
			//	    return new OtpErlangRef(this);

			case OtpExternal.portTag =>
				read_port()

			case OtpExternal.pidTag =>
				read_pid()

			case OtpExternal.stringTag =>
				read_string()

			case (OtpExternal.listTag | OtpExternal.nilTag) =>
				if ((flags & DECODE_INT_LISTS_AS_STRINGS) != 0) {
					val savePos = getPos();
					try {
						return read_string()
					} catch {
						case e: Exception =>
					}
					setPos(savePos);
				}
				return read_list()

			case (OtpExternal.smallTupleTag | OtpExternal.largeTupleTag) =>

				val arity = read_tuple_head();
				if (arity > 0) {
					read_tuple(arity)
				} else {
					() //return a empty tuple ()
				}

			case OtpExternal.binTag =>
				read_binary()
			//
			case OtpExternal.bitBinTag =>
				read_bit_str()

			//	case OtpExternal.compressedTag:
			//	    return read_compressed();

			//	case OtpExternal.newFunTag:
			//	case OtpExternal.funTag:
			//	    return new OtpErlangFun(this);

			case _ =>
				throw new Exception("Uknown data type: " + tag);
		}
	}

	/**
	 * read erlang bitstr. An Erlang bitstr is an
	 * Erlang binary with a length not an integral number of bytes (8-bit). Anything
	 * can be represented as a sequence of bytes can be made into an Erlang bitstr.
	 */
	def read_bit_str(): Array[Byte] = {
		val pbs = Array.apply(0)
		val bin = read_bitstr(pbs)
		val pad_bits = pbs(0)

		if (pad_bits < 0 || 7 < pad_bits) {
			throw new java.lang.IllegalArgumentException(
				"Padding must be in range 0..7")
		}
		if (pad_bits != 0 && bin.length == 0) {
			throw new java.lang.IllegalArgumentException(
				"Padding on zero length bitstr")
		}
		if (bin.length != 0) {
			bin(bin.length - 1) = (bin(bin.length - 1) & (~((1 << pad_bits) - 1))).asInstanceOf[Byte]
		}
		bin
	}

	/**
	 * read tuple from channel
	 */
	def read_tuple(arity: Int): Any = arity match {
		case 1 => (read_any())
		case 2 => (read_any(), read_any())
		case 3 => (read_any(), read_any(), read_any())
		case 4 => (read_any(), read_any(), read_any(), read_any())
		case 5 => (read_any(), read_any(), read_any(), read_any(), read_any())
		case 6 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 7 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 8 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 9 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 10 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 11 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 12 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 13 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 14 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 15 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 16 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 17 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 18 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 19 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 20 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 21 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case 22 => (read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any(), read_any())
		case _ => readBigTuple(arity) //超过22 元素的元组，特殊处理
	}

	def readBigTuple(arity: Int): Product = {
		val key = read_any()
		val elements = (for (n <- (1 until arity)) yield {
			read_any()
		}).toSeq
		Tuple2(key, elements)
	}
}

object OtpInputStream {
	def apply(bufs: Array[Byte], offset: Int, length: Int, flags: Int) = {
		val is = new OtpInputStream(bufs)
		is._otpInputStream(bufs, offset, length, flags)
		is
	}

	def apply(bufs: Array[Byte], flags: Int) = {
		val is = new OtpInputStream(bufs)
		is._otpInputStream(bufs)
		is
	}

	def apply(bufs: Array[Byte]) = {
		val is = new OtpInputStream(bufs)
		is._otpInputStream(bufs)
		is
	}
}