package com.qifun.otp.erlang

case class OtpErlangExit(reason : String) extends Exception(reason)