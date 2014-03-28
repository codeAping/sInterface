package com.qifun.otp.erlang

// import java.io.OutputStream;
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.UnsupportedEncodingException
import java.math.BigDecimal
import java.math.BigInteger
import java.text.DecimalFormat
import java.util.Arrays
import java.util.zip.Deflater
import scala.util.control.Breaks._
import javax.sound.sampled.Port
import com.qifun.infrastructure.DataObject
import scala.collection.mutable.ArrayBuffer
import com.mongodb.bson.BSONObjectID
import scala.collection.immutable.Stream
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ListBuffer

/**
 * scala <----> erlang 编码模块
 * 提供各种数据类型的发送接口
 */
class OtpOutputStream(defaultInitialSize: Int = 2048) extends ByteArrayOutputStream(defaultInitialSize) {
	/** The default initial size of the stream. * */
	//    val defaultInitialSize = 2048;

	/** The default increment used when growing the stream (increment at least this much). * */
	val defaultIncrement = 2048;

	// static formats, used to encode floats and doubles
	val eform = new DecimalFormat("e+00;e-00");
	val ten = new BigDecimal(10.0);
	val one = new BigDecimal(1.0);

	var fixedSize = Integer.MAX_VALUE;

	//    /**
	//     * Create a stream with the default initial size (2048 bytes).
	//     */
	def _otpOutputStream() {
		_otpOutputStream(defaultInitialSize);
	}
	//
	//    /**
	//     * Create a stream with the specified initial size.
	//     */
	def _otpOutputStream(size: Int) {
		buf = new Array[Byte](defaultIncrement)
	}
	//
	//    /**
	//     * Create a stream containing the encoded version of the given Erlang term.
	//     */
	def _otpOutputStream(o: Any) {
		_otpOutputStream()
		write_any(o);
	}

	implicit def fromDouble(d : Any):Double = {
		d.asInstanceOf[Double].toLong
	}
	/*
     * Get the contents of the output stream as an input stream instead. This is
     * used internally in {@link OtpCconnection} for tracing outgoing packages.
     * 
     * @param offset where in the output stream to read data from when creating
     * the input stream. The offset is necessary because header contents start 5
     * bytes into the header buffer, whereas payload contents start at the
     * beginning
     * 
     * @return an input stream containing the same raw data.
     */
	def getOtpInputStream(offset: Int): OtpInputStream = {
		OtpInputStream(buf, offset, count - offset, 0)
	}

	/**
	 * Get the current position in the stream.
	 *
	 * @return the current position in the stream.
	 */
	def getPos(): Int = {
		count
	}

	/**
	 * Trims the capacity of this <tt>OtpOutputStream</tt> instance to be the
	 * buffer's current size.  An application can use this operation to minimize
	 * the storage of an <tt>OtpOutputStream</tt> instance.
	 */
	def trimToSize() {
		resize(count)
	}

	def resize(size: Int) {
		if (size < buf.length) {
			val tmp = new Array[Byte](size)
			System.arraycopy(buf, 0, tmp, 0, size)
			buf = tmp;
		} else if (size > buf.length) {
			ensureCapacity(size)
		}
	}

	/**
	 * Increases the capacity of this <tt>OtpOutputStream</tt> instance, if
	 * necessary, to ensure that it can hold at least the number of elements
	 * specified by the minimum capacity argument.
	 *
	 * @param   minCapacity   the desired minimum capacity
	 */
	def ensureCapacity(minCapacity: Int) {
		if (minCapacity > fixedSize) {
			throw new IllegalArgumentException("Trying to increase fixed-size buffer");
		}
		var oldCapacity = buf.length;
		if (minCapacity > oldCapacity) {
			var newCapacity = (oldCapacity * 3) / 2 + 1
			if (newCapacity < oldCapacity + defaultIncrement)
				newCapacity = oldCapacity + defaultIncrement
			if (newCapacity < minCapacity)
				newCapacity = minCapacity
			newCapacity = Math.min(fixedSize, newCapacity)
			// minCapacity is usually close to size, so this is a win:
			val tmp = new Array[Byte](newCapacity)
			System.arraycopy(buf, 0, tmp, 0, count)
			buf = tmp
		}
	}

	/**
	 * Write one byte to the stream.
	 *
	 * @param b
	 *            the byte to write.
	 *
	 */
	def write(b: scala.Byte) {
		ensureCapacity(count + 1)
		buf(count) = b
		count += 1
	}

	/* (non-Javadoc)
     * @see java.io.ByteArrayOutputStream#write(byte[])
     */

	override def write(buf: Array[Byte]) {
		// don't assume that super.write(byte[]) calls write(buf, 0, buf.length)
		write(buf, 0, buf.length)
	}

	val lock = new ReentrantLock
	/* (non-Javadoc)
     * @see java.io.ByteArrayOutputStream#write(int)
     */
	override def write(b: Int) /*: Unit = synchronized*/{
		try{
			lock.lock()
			ensureCapacity(count + 1)
			buf(count) = b.asInstanceOf[Byte]
			count += 1
		}finally{
			lock.unlock()
		}
	}

	override def write(b: Array[Byte], off: Int, len: Int) : Unit = synchronized{
		try{
			lock.lock()
			if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
				throw new IndexOutOfBoundsException()
			}
			ensureCapacity(count + len)
			System.arraycopy(b, off, buf, count, len)
			count += len
		}finally{
			lock.unlock()
		}
	}

	/**
	 * Write the low byte of a value to the stream.
	 *
	 * @param n
	 *            the value to use.
	 *
	 */
	def write1(n: Long) {
		write((n & 0xff).asInstanceOf[Byte])
	}

	/**
	 * Write an array of bytes to the stream.
	 *
	 * @param buf
	 *            the array of bytes to write.
	 *
	 */
	def writeN(bytes: Array[Byte]) {
		write(bytes)
	}

	/**
	 * Get the current capacity of the stream. As bytes are added the capacity
	 * of the stream is increased automatically, however this method returns the
	 * current size.
	 *
	 * @return the size of the internal buffer used by the stream.
	 */
	def length(): Int = {
		buf.length
	}

	/**
	 * Write the low two bytes of a value to the stream in big endian order.
	 *
	 * @param n
	 *            the value to use.
	 */
	def write2BE(n: Long) {
		write(((n & 0xff00) >> 8).asInstanceOf[Byte])
		write((n & 0xff).asInstanceOf[Byte])
	}

	/**
	 * Write the low four bytes of a value to the stream in big endian order.
	 *
	 * @param n
	 *            the value to use.
	 */
	def write4BE(n: Long) {
		write(((n & 0xff000000) >> 24).asInstanceOf[Byte])
		write(((n & 0xff0000) >> 16).asInstanceOf[Byte])
		write(((n & 0xff00) >> 8).asInstanceOf[Byte])
		write((n & 0xff).asInstanceOf[Byte])
	}

	/**
	 * Write the low eight (all) bytes of a value to the stream in big endian
	 * order.
	 *
	 * @param n
	 *            the value to use.
	 */
	def write8BE(n: Long) {
		write((n >> 56 & 0xff).asInstanceOf[Byte])
		write((n >> 48 & 0xff).asInstanceOf[Byte])
		write((n >> 40 & 0xff).asInstanceOf[Byte])
		write((n >> 32 & 0xff).asInstanceOf[Byte])
		write((n >> 24 & 0xff).asInstanceOf[Byte])
		write((n >> 16 & 0xff).asInstanceOf[Byte])
		write((n >> 8 & 0xff).asInstanceOf[Byte])
		write((n & 0xff).asInstanceOf[Byte])
	}

	/**
	 * Write any number of bytes in little endian format.
	 *
	 * @param n
	 *            the value to use.
	 * @param b
	 *            the number of bytes to write from the little end.
	 */
	def writeLE(n: Long, b: Int) {
		var tmp = n
		for (i <- 0 until b) {
			write((tmp & 0xff).asInstanceOf[Byte])
			tmp >>= 8
		}
	}

	/**
	 * Write the low two bytes of a value to the stream in little endian order.
	 *
	 * @param n
	 *            the value to use.
	 */
	def write2LE(n: Long) {
		write((n & 0xff).asInstanceOf[Byte])
		write(((n & 0xff00) >> 8).asInstanceOf[Byte])
	}

	/**
	 * Write the low four bytes of a value to the stream in little endian order.
	 *
	 * @param n
	 *            the value to use.
	 */
	def write4LE(n: Long) {
		write((n & 0xff).asInstanceOf[Byte])
		write(((n & 0xff00) >> 8).asInstanceOf[Byte])
		write(((n & 0xff0000) >> 16).asInstanceOf[Byte])
		write(((n & 0xff000000) >> 24).asInstanceOf[Byte])
	}

	/**
	 * Write the low eight bytes of a value to the stream in little endian
	 * order.
	 *
	 * @param n
	 *            the value to use.
	 */
	def write8LE(n: Long) {
		write((n & 0xff).asInstanceOf[Byte])
		write((n >> 8 & 0xff).asInstanceOf[Byte])
		write((n >> 16 & 0xff).asInstanceOf[Byte])
		write((n >> 24 & 0xff).asInstanceOf[Byte])
		write((n >> 32 & 0xff).asInstanceOf[Byte])
		write((n >> 40 & 0xff).asInstanceOf[Byte])
		write((n >> 48 & 0xff).asInstanceOf[Byte])
		write((n >> 56 & 0xff).asInstanceOf[Byte])
	}

	/**
	 * Write the low four bytes of a value to the stream in bif endian order, at
	 * the specified position. If the position specified is beyond the end of
	 * the stream, this method will have no effect.
	 *
	 * Normally this method should be used in conjunction with {@link #size()
	 * size()}, when is is necessary to insert data into the stream before it is
	 * known what the actual value should be. For example:
	 *
	 * <pre>
	 * int pos = s.size();
	 *    s.write4BE(0); // make space for length data,
	 *                   // but final value is not yet known
	 *     [ ...more write statements...]
	 *    // later... when we know the length value
	 *    s.poke4BE(pos, length);
	 * </pre>
	 *
	 *
	 * @param offset
	 *            the position in the stream.
	 * @param n
	 *            the value to use.
	 */
	def poke4BE(offset: Int, n: Long) {
		if (offset < count) {
			buf(offset + 0) = ((n & 0xff000000) >> 24).asInstanceOf[Byte]
			buf(offset + 1) = ((n & 0xff0000) >> 16).asInstanceOf[Byte]
			buf(offset + 2) = ((n & 0xff00) >> 8).asInstanceOf[Byte]
			buf(offset + 3) = (n & 0xff).asInstanceOf[Byte]
		}
	}

	/**
	 * Write a Symbol to the stream as an Erlang atom.
	 *
	 * @param atom
	 *            the string to write.
	 */
	def write_atom(s: Symbol) {
		var atom = s.name
		var enc_atom: String = null
		var bytes: Array[Byte] = null
		var isLatin1 = true

		if (atom.codePointCount(0, atom.length()) <= OtpExternal.maxAtomLength) {
			enc_atom = atom;
		} else {
			enc_atom = new String(OtpErlangString.stringToCodePoints(atom),	0, OtpExternal.maxAtomLength)
		}
		breakable {
			var offset = 0
			while (offset < enc_atom.length()) {
				val cp = enc_atom.codePointAt(offset)
				if ((cp & ~0xFF) != 0) {
					isLatin1 = false
					break
				}
				offset = offset + Character.charCount(cp)
			}
		}
		if (isLatin1) {
			bytes = enc_atom.getBytes("ISO-8859-1")
			write1(OtpExternal.atomTag)
			write2BE(bytes.length)
		} else {
			bytes = enc_atom.getBytes("UTF-8")
			val length = bytes.length
			if (length < 256) {
				write1(OtpExternal.smallAtomUtf8Tag)
				write1(length)
			} else {
				write1(OtpExternal.atomUtf8Tag)
				write2BE(length)
			}
		}
		writeN(bytes)
	}
	/**
	 * Write a string to the stream as an Erlang atom.
	 *
	 * @param atom
	 *            the string to write.
	 */
	def write_atom(atom: String) {
		var enc_atom: String = null
		var bytes: Array[Byte] = null
		var isLatin1 = true

		if (atom.codePointCount(0, atom.length()) <= OtpExternal.maxAtomLength) {
			enc_atom = atom
		} else {
			enc_atom = new String(OtpErlangString.stringToCodePoints(atom), 0, OtpExternal.maxAtomLength)
		}
		breakable {
			var offset = 0
			while (offset < enc_atom.length()) {
				val cp = enc_atom.codePointAt(offset)
				if ((cp & ~0xFF) != 0) {
					isLatin1 = false
					break
				}
				offset = offset + Character.charCount(cp)
			}
		}

		try {
			if (isLatin1) {
				bytes = enc_atom.getBytes("ISO-8859-1")
				write1(OtpExternal.atomTag)
				write2BE(bytes.length)
			} else {
				bytes = enc_atom.getBytes("UTF-8")
				val length = bytes.length
				if (length < 256) {
					write1(OtpExternal.smallAtomUtf8Tag)
					write1(length)
				} else {
					write1(OtpExternal.atomUtf8Tag)
					write2BE(length)
				}
			}
			writeN(bytes)
		} catch {
			case e: UnsupportedEncodingException =>
				write1(OtpExternal.smallAtomUtf8Tag)
				write1(2)
				write2BE(0xffff) /* Invalid UTF-8 */
		}
	}

	/**
	 * Write an array of bytes to the stream as an Erlang binary.
	 *
	 * @param bin
	 *            the array of bytes to write.
	 */
	def write_binary(bin: Array[Byte]) {
		write1(OtpExternal.binTag)
		write4BE(bin.length)
		writeN(bin)
	}

	/**
	 * Write an array of bytes to the stream as an Erlang bitstr.
	 *
	 * @param bin
	 *            the array of bytes to write.
	 * @param pad_bits
	 *            the number of zero pad bits at the low end of the last byte
	 */
	def write_bitstr(bin: Array[Byte], pad_bits: Int) {
		if (pad_bits == 0) {
			write_binary(bin)
			return
		}
		write1(OtpExternal.bitBinTag)
		write4BE(bin.length)
		write1(8 - pad_bits)
		writeN(bin)
	}

	/**
	 * Write a boolean value to the stream as the Erlang atom 'true' or 'false'.
	 *
	 * @param b
	 *            the boolean value to write.
	 */
	def write_boolean(b: Boolean) {
		write_atom(String.valueOf(b))
	}

	/**
	 * Write a single byte to the stream as an Erlang integer. The byte is
	 * really an IDL 'octet', that is, unsigned.
	 *
	 * @param b
	 *            the byte to use.
	 */
	def write_byte(b: Byte) {
		this.write_long(b & 0xffL, true)
	}

	/**
	 * Write a character to the stream as an Erlang integer. The character may
	 * be a 16 bit character, kind of IDL 'wchar'. It is up to the Erlang side
	 * to take care of souch, if they should be used.
	 *
	 * @param c
	 *            the character to use.
	 */
	def write_char(c: Char) {
		this.write_long(c & 0xffffL, true)
	}

	/**
	 * Write a double value to the stream.
	 *
	 * @param d
	 *            the double to use.
	 */
	def write_double(d: Double) {
		write1(OtpExternal.newFloatTag)
		write8BE(java.lang.Double.doubleToLongBits(d))
	}

	/**
	 * Write a float value to the stream.
	 *
	 * @param f
	 *            the float to use.
	 */
	def write_float(f: Float) {
		write_double(f)
	}

	def write_big_integer(v: BigInteger) {
		var tmp = v
		if (tmp.bitLength() < 64) {
			this.write_long(tmp.longValue(), true)
			return
		}
		val signum = tmp.signum()
		if (signum < 0) {
			tmp = tmp.negate()
		}
		val magnitude = tmp.toByteArray()
		val n = magnitude.length
		// Reverse the array to make it little endian.

		var i = 0
		var j = n
		while (i < j) {
			val b = magnitude(i)
			magnitude(i) = magnitude(j)
			magnitude(j) = b
			i = i + 1
			j = j - 1
		}

		if ((n & 0xFF) == n) {
			write1(OtpExternal.smallBigTag)
			write1(n) // length
		} else {
			write1(OtpExternal.largeBigTag)
			write4BE(n) // length
		}
		var k = 0
		if (signum > 0) k = 1
		write1(k) // sign
		// Write the array
		writeN(magnitude)
	}

	def write_long(v: Long, unsigned: Boolean) {
		/*
	 * If v<0 and unsigned==true the value
	 * java.lang.Long.MAX_VALUE-java.lang.Long.MIN_VALUE+1+v is written, i.e
	 * v is regarded as unsigned two's complement.
	 */
		if ((v & 0xffL) == v) {
			// will fit in one byte
			write1(OtpExternal.smallIntTag)
			write1(v)
		} else {
			// note that v != 0L
			if (v < 0 && unsigned || v < OtpExternal.erlMin	|| v > OtpExternal.erlMax) {
				// some kind of bignum
				var abs = v
				if (!unsigned) {
					if (v < 0) abs = -v
				}
				var sign = 0
				if (!unsigned) {
					if (v < 0) sign = 1
				}

				var n = 4;
				var mask = 0xFFFFffffL;
				while ((abs & mask) != abs) {
					n = n + 1
					mask = mask << 8 | 0xffL
				}
				write1(OtpExternal.smallBigTag)
				write1(n) // length
				write1(sign) // sign
				writeLE(abs, n) // value. obs! little endian
			} else {
				write1(OtpExternal.intTag)
				write4BE(v)
			}
		}
	}

	/**
	 * Write a long to the stream.
	 *
	 * @param l
	 *            the long to use.
	 */
	def write_long(l: Long) {
		this.write_long(l, false)
	}

	/**
	 * Write a positive long to the stream. The long is interpreted as a two's
	 * complement unsigned long even if it is negative.
	 *
	 * @param ul
	 *            the long to use.
	 */
	def write_ulong(ul: Long) {
		this.write_long(ul, true)
	}

	/**
	 * Write an integer to the stream.
	 *
	 * @param i
	 *            the integer to use.
	 */
	def write_int(i: Int) {
		this.write_long(i, false)
	}

	/**
	 * Write a positive integer to the stream. The integer is interpreted as a
	 * two's complement unsigned integer even if it is negative.
	 *
	 * @param ui
	 *            the integer to use.
	 */
	def write_uint(ui: Int) {
		this.write_long(ui & 0xFFFFffffL, true)
	}

	/**
	 * Write a short to the stream.
	 *
	 * @param s
	 *            the short to use.
	 */
	def write_short(s: Short) {
		this.write_long(s, false)
	}

	/**
	 * Write a positive short to the stream. The short is interpreted as a two's
	 * complement unsigned short even if it is negative.
	 *
	 * @param s
	 *            the short to use.
	 */
	def write_ushort(us: Short) {
		this.write_long(us & 0xffffL, true)
	}

	/**
	 * Write an Erlang list header to the stream. After calling this method, you
	 * must write 'arity' elements to the stream followed by nil, or it will not
	 * be possible to decode it later.
	 *
	 * @param arity
	 *            the number of elements in the list.
	 */
	def write_list_head(arity: Int) {
		if (arity == 0) {
			write_nil()
		} else {
			write1(OtpExternal.listTag)
			write4BE(arity)
		}
	}

	/**
	 * Write an empty Erlang list to the stream.
	 */
	def write_nil() {
		write1(OtpExternal.nilTag)
	}

	/**
	 * Write an Erlang tuple header to the stream. After calling this method,
	 * you must write 'arity' elements to the stream or it will not be possible
	 * to decode it later.
	 *
	 * @param arity
	 *            the number of elements in the tuple.
	 */
	def write_tuple_head(arity: Int) {
		if (arity < 0xff) {
			write1(OtpExternal.smallTupleTag)
			write1(arity)
		} else {
			write1(OtpExternal.largeTupleTag)
			write4BE(arity)
		}
	}

	/**
	 * Write an Erlang PID to the stream.
	 *
	 * @param node
	 *            the nodename.
	 *
	 * @param id
	 *            an arbitrary number. Only the low order 15 bits will be used.
	 *
	 * @param serial
	 *            another arbitrary number. Only the low order 13 bits will be
	 *            used.
	 *
	 * @param creation
	 *            yet another arbitrary number. Only the low order 2 bits will
	 *            be used.
	 *
	 */
	def write_pid(node: String, id: Int, serial: Int, creation: Int) {
		write1(OtpExternal.pidTag)
		write_atom(node)
		write4BE(id & 0x7fff) // 15 bits
		write4BE(serial & 0x1fff) // 13 bits
		write1(creation & 0x3) // 2 bits
	}

	/**
	 * Write an Erlang port to the stream.
	 *
	 * @param node
	 *            the nodename.
	 *
	 * @param id
	 *            an arbitrary number. Only the low order 28 bits will be used.
	 *
	 * @param creation
	 *            another arbitrary number. Only the low order 2 bits will be
	 *            used.
	 *
	 */
	def write_port(node: String, id: Int, creation: Int) {
		write1(OtpExternal.portTag)
		write_atom(node)
		write4BE(id & 0xfffffff) // 28 bits
		write1(creation & 0x3) // 2 bits
	}

	/**
	 * Write an old style Erlang ref to the stream.
	 *
	 * @param node
	 *            the nodename.
	 *
	 * @param id
	 *            an arbitrary number. Only the low order 18 bits will be used.
	 *
	 * @param creation
	 *            another arbitrary number. Only the low order 2 bits will be
	 *            used.
	 *
	 */
	def write_ref(node: String, id: Int, creation: Int) {
		write1(OtpExternal.refTag)
		write_atom(node)
		write4BE(id & 0x3ffff) // 18 bits
		write1(creation & 0x3) // 2 bits
	}

	/**
	 * Write a new style (R6 and later) Erlang ref to the stream.
	 *
	 * @param node
	 *            the nodename.
	 *
	 * @param ids
	 *            an array of arbitrary numbers. Only the low order 18 bits of
	 *            the first number will be used. If the array contains only one
	 *            number, an old style ref will be written instead. At most
	 *            three numbers will be read from the array.
	 *
	 * @param creation
	 *            another arbitrary number. Only the low order 2 bits will be
	 *            used.
	 *
	 */
	def write_ref(node: String, ids: Array[Int], creation: Int) {
		var arity = ids.length;
		if (arity > 3) {
			arity = 3; // max 3 words in ref
		}

		if (arity == 1) {
			// use old method
			this.write_ref(node, ids(0), creation)
		} else {
			// r6 ref
			write1(OtpExternal.newRefTag)

			// how many id values
			write2BE(arity)

			write_atom(node)

			// note: creation BEFORE id in r6 ref
			write1(creation & 0x3); // 2 bits

			// first int gets truncated to 18 bits
			write4BE(ids(0) & 0x3ffff);

			// remaining ones are left as is
			for (i <- 1 until arity) {
				write4BE(ids(i))
			}
		}
	}

	/**
	 * Write a string to the stream.
	 *
	 * @param s
	 *            the string to write.
	 */
	def write_string(s: String) {
		val len = s.length()

		len match {
			case 0 =>
				write_nil()
			case _ =>
				if (len <= 65535 && is8bitString(s)) { // 8-bit string
					try {
						val bytebuf = s.getBytes("ISO-8859-1")
						write1(OtpExternal.stringTag)
						write2BE(len)
						writeN(bytebuf)
					}catch {
						case e :UnsupportedEncodingException => write_nil()
					}
				} else { // unicode or longer, must code as list
					val charbuf = s.toCharArray();
					val codePoints = OtpErlangString.stringToCodePoints(s);
					write_list_head(codePoints.length);
					codePoints foreach {
						write_int(_)
					}
					write_nil()
				}
		}
	}

	private[this] def is8bitString(s: String): Boolean = {
		val len = s.length()
		for (i <- 0 until len) {
			val c = s.charAt(i);
			if (c < 0 || c > 255) {
				return false;
			}
		}
		return true;
	}


	/**
	 * 发送一个列表数据
	 */
	def write_list(l: List[Any]) {
		val arity = l.length
		if(arity > 0){
			write_list_head(arity)
			l foreach {
				p => write_any(p)
			}
		}
		write_nil()
	}
	
	/**
	 * 发送一个列表数据
	 */
	def write_set(l: List[Any]) {
		val arity = l.length
		if(arity > 0){
			write_list_head(arity)
			l foreach {
				p => write_any(p)
			}
		}
		
		
		write_nil()
	}

	/**
	 * 发送一个列表数据
	 */
	def write_array_buffer(ar : ArrayBuffer[Any]) {
		println(ar.toList)
		write_list(ar.toList)
	}
	
	/**
	 * 发送一个列表数据
	 */
	def write_list_buffer(ar : ListBuffer[Any]) {
		write_list(ar.toList)
	}
	
	/**
	 * 发送一个数组数据
	 */
	def write_array(a: Array[Any]) {
		val arity = a.length
		if(arity > 0){
			write_list_head(arity)
			a foreach {
				p => write_any(p)
			}
		}
		write_nil()
	}

	/**
	 * 发送一个数组数据
	 */
	def write_map(m: collection.mutable.Map[Any,Any]) {
//		println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+m)
		val tups = m.toList
		write_any(Tuple1(tups))
	}
	/**
	 * 发送一个数组数据
	 */
	def write_map(m: collection.immutable.Map[Any,Any]) {
//		println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+m)
		val tups = m.toList
		write_any(Tuple1(tups))
	}
	/**
	 * 发送数据统一接口
	 */
	def write_any(o: Any): Unit =  {
	o match{
		case b: Byte =>
			write_byte(b)
		case s: Short =>
			write_short(s)
		case c: Char =>
			write_char(c)
		case i: Int =>
			write_int(i)
		case l: Long =>
			write_long(l)
		case f: Float =>
			write_float(f)
		case d: Double =>
			write_double(d)
		case true =>
			write_atom('true)
		case false =>
			write_atom('false)
		case ls: List[Any] =>
			write_list(ls)
	    case ls: ListBuffer[Any @unchecked] =>
			write_list_buffer(ls)
		case s:Stream[Any] =>
			write_set(s.toList)
		case ls: scala.collection.mutable.Set[Any @unchecked] =>
			write_set(ls.toList)
		case ar:ArrayBuffer[Any @unchecked] =>
			write_array_buffer(ar)
		case ar: Array[Byte] =>
			write_binary(ar)
		case a: Array[Any] =>
			write_array(a)
		case m : collection.mutable.Map[Any,Any]@unchecked =>
			write_map(m)
		case m : collection.immutable.Map[Any,Any]@unchecked =>
			write_map(m)
		case s: String =>
			write_binary(s.getBytes())
//			write_string(s)
		case sy: Symbol =>
			write_atom(sy)
		case OtpErlangPid(node, id, serial, creation) =>
			write_pid(node.name, id, serial, creation)
		case OtpErlangPort(node, id, creation) =>
			write_port(node.name, id, creation)
//		case id: ObjectId =>
//			write_binary(id.toByteArray())
		case id:BSONObjectID =>
			write_binary(id.value)
		case p: Product =>
			writeProduct(p)

		case x @ _ => System.err.println(s"# send data type error,,please check the data $x");
	}
	}

	private val ok = Symbol("ok")
	private val error = Symbol("error")
	
//	implicit def fromEither(e: Either[Any, Any]) = e match {
//	   case Left(l) => Tuple2(error, l)
//	   case Right(r) => Tuple2(ok, r)
//	}
	
	def writeProduct(p: Product) {
		val name = p.productPrefix
		if (name.startsWith("Tuple")) {
			writeTuple(None, p)
		} else {
			writeObject(p)	
		}
	}
	
	def writeObject( p: Product) = p match{
		case Left(l) => write_any(Tuple2(error, l))
		case Right(r) =>write_any(Tuple2(ok, r))
		case _ =>
			write_tuple_head(p.productArity + 1)
			write_atom(p.productPrefix.toLowerCase)
			for (element <- p.productIterator) {
				if(element.isInstanceOf[Double]){
					write_any(element.asInstanceOf[Double].toLong)
	//				write_long(element)
				}else{
					write_any(element)
				}
			}
	}
	
	def writeTuple(tag: Option[String], p: Product) {
		val length = p.productArity
//		val length = tag.size + p.productArity
		write_tuple_head(length)
		for (element <- p.productIterator) {
			if(element.isInstanceOf[Double]){
				write_any(element.asInstanceOf[Double].toLong)
			}else{
				write_any(element)
			}
		}
	}

}

object OtpOutputStream {
	def apply(o: Any) = {
		val os = new OtpOutputStream
		os._otpOutputStream(o)
		os
	}

	def apply(i: Int) = {
		val os = new OtpOutputStream
		os._otpOutputStream()
		os
	}
}