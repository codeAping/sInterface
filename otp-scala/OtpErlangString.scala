package com.qifun.otp.erlang;

import java.io.Serializable;
import java.lang.Character;
import java.io.UnsupportedEncodingException;


object OtpErlangString {
    /**
     * Create Unicode code points from a String.
     * 
     * @param  s
     *             a String to convert to an Unicode code point array
     *
     * @return the corresponding array of integers representing
     *         Unicode code points
     */

   def stringToCodePoints(s : String ) : Array[Int] = {
        val m = s.codePointCount(0, s.length())
        val codePoints = new Array[Int](m)
        var j = 0
        var offset = 0
        while(offset < s.length()){
        	var codepoint = s.codePointAt(offset)
        	codePoints(j) = codepoint
            offset += Character.charCount(codepoint)
            j += 1
        }
        codePoints
    }

    

    def isValidCodePoint(cp : Int) : Boolean = {
		(cp>>>16) <= 0x10 && (cp & ~0x7FF) != 0xD800
    }

    /**
     * Construct a String from a Latin-1 (ISO-8859-1) encoded byte array,
     * if Latin-1 is available, otherwise use the default encoding. 
     *
     */
    def newString(bytes : Array[Byte]) : String ={
		return new String(bytes)
    }
}
