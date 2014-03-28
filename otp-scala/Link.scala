package com.qifun.otp.erlang

case class Link(local : OtpErlangPid , remote : OtpErlangPid ) {
    var hashCodeValue = 0;

    def locals() : OtpErlangPid ={
    	local
    }

    def remotes() : OtpErlangPid ={
    	remote
    }

    /**
     * 检查PID是否已存在
     */
    def contains(pid : OtpErlangPid)  : Boolean ={
    	local.equals(pid) || remote.equals(pid)
    }

    /**
     * 判断2个PID 是否是同一个
     */
    def equals(local : OtpErlangPid, remote : OtpErlangPid ) : Boolean ={
    	(local.id == remote.id && local.serial == remote.serial && local.creation == remote.creation && local.node == remote.node)
//		(local.equals(local) && this.remote.equals(remote)) || (local.equals(remote) && this.remote.equals(local))
    }
   
}