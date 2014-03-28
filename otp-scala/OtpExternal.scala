package com.qifun.otp.erlang

/**
 * erlang 与scala 数据类型tag 常量
 */
object OtpExternal {

	/** The tag used for small integers */
    val smallIntTag = 97

    /** The tag used for integers */
    val intTag = 98

    /** The tag used for floating point numbers */
    val floatTag = 99
    val newFloatTag = 70

    /** The tag used for atoms */
    val atomTag = 100

    /** The tag used for old stype references */
    val refTag = 101

    /** The tag used for ports */
    val portTag = 102

    /** The tag used for PIDs */
    val pidTag = 103

    /** The tag used for small tuples */
    val smallTupleTag = 104

    /** The tag used for large tuples */
    val largeTupleTag = 105

    /** The tag used for empty lists */
    val nilTag = 106

    /** The tag used for strings and lists of small integers */
    val stringTag = 107

    /** The tag used for non-empty lists */
    val listTag = 108

    /** The tag used for binaries */
    val binTag = 109

    /** The tag used for bitstrs */
    val bitBinTag = 77

    /** The tag used for small bignums */
    val smallBigTag = 110

    /** The tag used for large bignums */
    val largeBigTag = 111

    /** The tag used for old new Funs */
    val newFunTag = 112

    /** The tag used for external Funs (M:F/A) */
    val externalFunTag = 113

    /** The tag used for new style references */
    val newRefTag = 114

    /** The tag used for old Funs */
    val funTag = 117

    /** The tag used for unicode atoms */
    val atomUtf8Tag = 118

    /** The tag used for small unicode atoms */
    val smallAtomUtf8Tag = 119

    /** The tag used for compressed terms */
    val compressedTag = 80

    /** The version number used to mark serialized Erlang terms */
    val versionTag = 131

    /** The largest value that can be encoded as an integer */
    val erlMax = (1 << 27) - 1

    /** The smallest value that can be encoded as an integer */
    val erlMin = -(1 << 27)

    /** The longest allowed Erlang atom */
    val maxAtomLength = 255
}