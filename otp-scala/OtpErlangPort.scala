package com.qifun.otp.erlang

/**
 * Provides a scala representation of Erlang ports.
 */
case class OtpErlangPort(node : Symbol, id : Int, creation : Int) {

  def toErlangString : String = {
    "#Port<" + id  + "." + creation + ">"
  }
  
  override def toString : String = {
    "#Port<" + id + "." + creation + ">"
  }
}
