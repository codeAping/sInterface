package com.qifun.otp.erlang

class OtpMD5 {

	/*
     * * MD5 constants
     */
	val S11 = 7L
	val S12 = 12L
	val S13 = 17L
	val S14 = 22L
	val S21 = 5L
	val S22 = 9L
	val S23 = 14L
	val S24 = 20L
	val S31 = 4L
	val S32 = 11L
	val S33 = 16L
	val S34 = 23L
	val S41 = 6L
	val S42 = 10L
	val S43 = 15L
	val S44 = 21L

	/*
     * Has to be this large to avoid sign problems
     */

	val state = Array.apply(0x67452301L, 0xefcdab89L, 0x98badcfeL, 0x10325476L)

	val count = Array.apply(0L, 0L)
	var buffer: Array[Int] = null

	def initOtpMD5() {
		buffer = new Array[Int](64)
		for (i <- 0 until 64) {
			buffer(i) = 0
		}
	}

	private def to_bytes(s: String): Array[Int] = {
		val tmp = s.toCharArray()
		val ret = new Array[Int](tmp.length)

		for (i <- 0 until tmp.length) {
			ret(i) = tmp(i) & 0xFF
		}
		ret
	}

	private def clean_bytes(bytes: Array[Int]): Array[Int] = {
		val ret = new Array[Int](bytes.length)

		for (i <- 0 until bytes.length) {
			ret(i) = bytes(i) & 0xFF
		}
		ret
	}

	/*
     * * A couple of operations where 32 bit over/under-flow is expected
     */

	private def shl(what: Long, steps: Int): Long = {
		return what << steps & 0xFFFFFFFFL
	}

	private def shr(what: Long, steps: Int): Long = {
		return what >>> steps
	}

	private def plus(a: Long, b: Long): Long = {
		return a + b & 0xFFFFFFFFL
	}

	private def not(x: Long): Long = {
		return ~x & 0xFFFFFFFFL
	}

	private def to_buffer(to_start: Int, from: Array[Int], from_start: Int,
		num: Int) {
		var t_num = num
		var t_to_start = to_start
		var t_from_start = from_start
		while (t_num > 0) {
			buffer(t_to_start) = from(t_from_start)
			t_num -= 1
			t_to_start += 1
			t_from_start += 1
		}
	}

	private def do_update(bytes: Array[Int]) {
		var index = (count(0) >>> 3 & 0x3F).asInstanceOf[Int]
		val inlen = bytes.length
		val addcount = shl(inlen, 3)
		val partlen = 64 - index
		var i = 0
		count(0) = plus(count(0), addcount)

		if (count(0) < addcount) {
			count(1) += 1
		}

		count(1) = plus(count(1), shr(inlen, 29))

		if (inlen >= partlen) {
			to_buffer(index, bytes, 0, partlen)
			transform(buffer, 0)

			i = partlen
			while (i + 63 < inlen) {
				transform(bytes, i)
				i += 64
			}
			index = 0
		} else {
			i = 0
		}
		to_buffer(index, bytes, i, inlen - i)
	}

	private def dumpstate() {
		System.out.println("state = {" + state(0) + ", " + state(1) + ", "
			+ state(2) + ", " + state(3) + "}");
		System.out.println("count = {" + count(0) + ", " + count(1) + "}");
		System.out.print("buffer = {")
		var i = 0;
		for (i <- 0 until 64) {
			if (i > 0) {
				System.out.print(", ")
			}
			System.out.print(buffer(i))
		}
		System.out.println("}")
	}

	/*
     * * The transformation functions
     */

	private def F(x: Long, y: Long, z: Long): Long = {
		x & y | not(x) & z
	}

	private def G(x: Long, y: Long, z: Long): Long = {
		x & z | y & not(z)
	}

	private def H(x: Long, y: Long, z: Long): Long = {
		x ^ y ^ z
	}

	private def I(x: Long, y: Long, z: Long): Long = {
		y ^ (x | not(z))
	}

	private def ROTATE_LEFT(x: Long, n: Long): Long = {
		shl(x, n.asInstanceOf[Int]) | shr(x, (32 - n).asInstanceOf[Int])
	}

	private def FF(a: Long, b: Long, c: Long, d: Long,
		x: Long, s: Long, ac: Long): Long = {
		var a1 = plus(a, plus(plus(F(b, c, d), x), ac))
		a1 = ROTATE_LEFT(a1, s)
		plus(a1, b)
	}

	private def GG(a: Long, b: Long, c: Long, d: Long,
		x: Long, s: Long, ac: Long): Long = {
		var a1 = plus(a, plus(plus(G(b, c, d), x), ac))
		a1 = ROTATE_LEFT(a1, s)
		plus(a1, b)
	}

	private def HH(a: Long, b: Long, c: Long, d: Long,
		x: Long, s: Long, ac: Long): Long = {
		var a1 = plus(a, plus(plus(H(b, c, d), x), ac))
		a1 = ROTATE_LEFT(a1, s)
		plus(a1, b)
	}

	private def II(a: Long, b: Long, c: Long, d: Long,
		x: Long, s: Long, ac: Long): Long = {
		var a1 = plus(a, plus(plus(I(b, c, d), x), ac))
		a1 = ROTATE_LEFT(a1, s)
		plus(a1, b)
	}

	private def decode(output: Array[Long], input: Array[Int],
		in_from: Int, len: Int) {
		var i = 0
		var j = 0
		while (j < len) {
			output(i) = input(j + in_from) | shl(input(j + in_from + 1), 8) | shl(input(j + in_from + 2), 16) | shl(input(j + in_from + 3), 24)

			i += 1
			j += 4
		}
	}

	private def transform(block: Array[Int], from: Int) {
		var a = state(0)
		var b = state(1)
		var c = state(2)
		var d = state(3)
		val x = new Array[Long](16)

		decode(x, block, from, 64)

		a = FF(a, b, c, d, x(0), S11, 0xd76aa478L) /* 1 */
		d = FF(d, a, b, c, x(1), S12, 0xe8c7b756L) /* 2 */
		c = FF(c, d, a, b, x(2), S13, 0x242070dbL) /* 3 */
		b = FF(b, c, d, a, x(3), S14, 0xc1bdceeeL) /* 4 */
		a = FF(a, b, c, d, x(4), S11, 0xf57c0fafL) /* 5 */
		d = FF(d, a, b, c, x(5), S12, 0x4787c62aL) /* 6 */
		c = FF(c, d, a, b, x(6), S13, 0xa8304613L) /* 7 */
		b = FF(b, c, d, a, x(7), S14, 0xfd469501L) /* 8 */
		a = FF(a, b, c, d, x(8), S11, 0x698098d8L) /* 9 */
		d = FF(d, a, b, c, x(9), S12, 0x8b44f7afL) /* 10 */
		c = FF(c, d, a, b, x(10), S13, 0xffff5bb1L) /* 11 */
		b = FF(b, c, d, a, x(11), S14, 0x895cd7beL) /* 12 */
		a = FF(a, b, c, d, x(12), S11, 0x6b901122L) /* 13 */
		d = FF(d, a, b, c, x(13), S12, 0xfd987193L) /* 14 */
		c = FF(c, d, a, b, x(14), S13, 0xa679438eL) /* 15 */
		b = FF(b, c, d, a, x(15), S14, 0x49b40821L) /* 16 */

		/* Round 2 */
		a = GG(a, b, c, d, x(1), S21, 0xf61e2562L) /* 17 */
		d = GG(d, a, b, c, x(6), S22, 0xc040b340L) /* 18 */
		c = GG(c, d, a, b, x(11), S23, 0x265e5a51L) /* 19 */
		b = GG(b, c, d, a, x(0), S24, 0xe9b6c7aaL) /* 20 */
		a = GG(a, b, c, d, x(5), S21, 0xd62f105dL) /* 21 */
		d = GG(d, a, b, c, x(10), S22, 0x2441453L) /* 22 */
		c = GG(c, d, a, b, x(15), S23, 0xd8a1e681L) /* 23 */
		b = GG(b, c, d, a, x(4), S24, 0xe7d3fbc8L) /* 24 */
		a = GG(a, b, c, d, x(9), S21, 0x21e1cde6L) /* 25 */
		d = GG(d, a, b, c, x(14), S22, 0xc33707d6L) /* 26 */
		c = GG(c, d, a, b, x(3), S23, 0xf4d50d87L) /* 27 */
		b = GG(b, c, d, a, x(8), S24, 0x455a14edL) /* 28 */
		a = GG(a, b, c, d, x(13), S21, 0xa9e3e905L) /* 29 */
		d = GG(d, a, b, c, x(2), S22, 0xfcefa3f8L) /* 30 */
		c = GG(c, d, a, b, x(7), S23, 0x676f02d9L) /* 31 */
		b = GG(b, c, d, a, x(12), S24, 0x8d2a4c8aL) /* 32 */

		/* Round 3 */
		a = HH(a, b, c, d, x(5), S31, 0xfffa3942L) /* 33 */
		d = HH(d, a, b, c, x(8), S32, 0x8771f681L) /* 34 */
		c = HH(c, d, a, b, x(11), S33, 0x6d9d6122L) /* 35 */
		b = HH(b, c, d, a, x(14), S34, 0xfde5380cL) /* 36 */
		a = HH(a, b, c, d, x(1), S31, 0xa4beea44L) /* 37 */
		d = HH(d, a, b, c, x(4), S32, 0x4bdecfa9L) /* 38 */
		c = HH(c, d, a, b, x(7), S33, 0xf6bb4b60L) /* 39 */
		b = HH(b, c, d, a, x(10), S34, 0xbebfbc70L) /* 40 */
		a = HH(a, b, c, d, x(13), S31, 0x289b7ec6L) /* 41 */
		d = HH(d, a, b, c, x(0), S32, 0xeaa127faL) /* 42 */
		c = HH(c, d, a, b, x(3), S33, 0xd4ef3085L) /* 43 */
		b = HH(b, c, d, a, x(6), S34, 0x4881d05L) /* 44 */
		a = HH(a, b, c, d, x(9), S31, 0xd9d4d039L) /* 45 */
		d = HH(d, a, b, c, x(12), S32, 0xe6db99e5L) /* 46 */
		c = HH(c, d, a, b, x(15), S33, 0x1fa27cf8L) /* 47 */
		b = HH(b, c, d, a, x(2), S34, 0xc4ac5665L) /* 48 */

		/* Round 4 */
		a = II(a, b, c, d, x(0), S41, 0xf4292244L) /* 49 */
		d = II(d, a, b, c, x(7), S42, 0x432aff97L) /* 50 */
		c = II(c, d, a, b, x(14), S43, 0xab9423a7L) /* 51 */
		b = II(b, c, d, a, x(5), S44, 0xfc93a039L) /* 52 */
		a = II(a, b, c, d, x(12), S41, 0x655b59c3L) /* 53 */
		d = II(d, a, b, c, x(3), S42, 0x8f0ccc92L) /* 54 */
		c = II(c, d, a, b, x(10), S43, 0xffeff47dL) /* 55 */
		b = II(b, c, d, a, x(1), S44, 0x85845dd1L) /* 56 */
		a = II(a, b, c, d, x(8), S41, 0x6fa87e4fL) /* 57 */
		d = II(d, a, b, c, x(15), S42, 0xfe2ce6e0L) /* 58 */
		c = II(c, d, a, b, x(6), S43, 0xa3014314L) /* 59 */
		b = II(b, c, d, a, x(13), S44, 0x4e0811a1L) /* 60 */
		a = II(a, b, c, d, x(4), S41, 0xf7537e82L) /* 61 */
		d = II(d, a, b, c, x(11), S42, 0xbd3af235L) /* 62 */
		c = II(c, d, a, b, x(2), S43, 0x2ad7d2bbL) /* 63 */
		b = II(b, c, d, a, x(9), S44, 0xeb86d391L) /* 64 */

		state(0) = plus(state(0), a)
		state(1) = plus(state(1), b)
		state(2) = plus(state(2), c)
		state(3) = plus(state(3), d)
	}

	def update(bytes: Array[Int]) {
		do_update(clean_bytes(bytes))
	}

	def update(s: String) {
		do_update(to_bytes(s))
	}

	private def encode(input: Array[Long], len: Int): Array[Int] = {
		var output = new Array[Int](len)
		var i = 0
		var j = 0
		while (j < len) {
			output(j) = (input(i) & 0xff).asInstanceOf[Int];
			output(j + 1) = (input(i) >>> 8 & 0xff).asInstanceOf[Int]
			output(j + 2) = (input(i) >>> 16 & 0xff).asInstanceOf[Int]
			output(j + 3) = (input(i) >>> 24 & 0xff).asInstanceOf[Int]

			i += 1
			j += 4
		}
		output
	}

	def final_bytes(): Array[Int] = {
		val bits = encode(count, 8)
		var index = -1
		var padlen = -1
		var i = 0
		var padding: Array[Int] = null
		var digest: Array[Int] = null

		index = (count(0) >>> 3 & 0x3f).asInstanceOf[Int]
		if (index < 56) padlen = 56 - index
		else 120 - index
		/* padlen > 0 */
		padding = new Array[Int](padlen)
		padding(0) = 0x80

		for (i <- 1 until padlen) {
			padding(i) = 0
		}

		do_update(padding)

		do_update(bits)

		digest = encode(state, 16)

		digest
	}
}


object OtpMD5 {
	def apply(i : Int) = {
		val md5 = new OtpMD5
		md5.initOtpMD5
		md5
	}
}