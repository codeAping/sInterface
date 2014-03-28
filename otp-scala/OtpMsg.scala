package com.qifun.otp.erlang
trait ConstsOtpMsg {
	val linkTag = 1
    val sendTag = 2
    val exitTag = 3
    val unlinkTag = 4
    val regSendTag = 6
    /* val groupLeaderTag = 7 */
    val exit2Tag = 8
}
//object OtpMsg{
//	
//}

object OtpMsg extends ConstsOtpMsg{
	 // send has receiver pid but no sender information
	def apply(to : OtpErlangPid , paybuf : OtpInputStream ) = {
		val msg = new OtpMsg
		msg.intOtpMsg1(to,paybuf)
		msg
	}
	
	// send has receiver pid but no sender information
	def apply(to : OtpErlangPid ,payload : Any) = {
		val msg = new OtpMsg
		msg.initOtpMsg2(to,payload)
		msg
	}
	
	 // send_reg has sender pid and receiver name
	def apply(from : OtpErlangPid , toName : String ,paybuf : OtpInputStream ) = {
		val msg = new OtpMsg
		msg.initOtpMsg3(from,toName,paybuf)
		msg
	}
	
	// send_reg has sender pid and receiver name
	def apply(from : OtpErlangPid , toName : String ,payload : Any ) = {
		val msg = new OtpMsg
		msg.initOtpMsg4(from,toName ,payload)
		msg
	}
	
	// send_reg has sender pid and receiver name
	def apply(tag : Int ,from : OtpErlangPid ,to : OtpErlangPid ,reason : Any) = {
		val msg = new OtpMsg
		msg.initOtpMsg5(tag ,from ,to ,reason)
		msg
	}
	// send_reg has sender pid and receiver name
	def apply(tag : Int,from : OtpErlangPid,to : OtpErlangPid ) = {
		val msg = new OtpMsg
		msg.initOtpMsg7(tag,from,to)
		msg
	}
}

class OtpMsg extends ConstsOtpMsg{

    var tag = 0 // what type of message is this (send, link, exit etc)
    var paybuf : OtpInputStream = null
    var payload : Any = null
    var from: OtpErlangPid = null
    var to : OtpErlangPid = null
    var toName : String = null

    // send has receiver pid but no sender information
    def intOtpMsg1(to : OtpErlangPid , paybuf : OtpInputStream) {
		tag = sendTag;
		from = null
		this.to = to
		toName = null
		this.paybuf = paybuf
		payload = null
    }

    // send has receiver pid but no sender information
    def initOtpMsg2(to : OtpErlangPid ,payload : Any) {
		tag = sendTag
		from = null
		this.to = to
		toName = null
		paybuf = null
		this.payload = payload
    }

    // send_reg has sender pid and receiver name
    def initOtpMsg3(from : OtpErlangPid , toName : String ,paybuf : OtpInputStream ) {
		tag = regSendTag;
		this.from = from;
		this.toName = toName;
		to = null;
		this.paybuf = paybuf;
		payload = null;
    }

    // send_reg has sender pid and receiver name
    def initOtpMsg4(from : OtpErlangPid , toName : String ,payload : Any ) {
		tag = regSendTag;
		this.from = from;
		this.toName = toName;
		to = null;
		paybuf = null;
		this.payload = payload;
    }

    // exit (etc) has from, to, reason
    def initOtpMsg5(tag : Int ,from : OtpErlangPid ,to : OtpErlangPid ,reason : Any) {
		this.tag = tag;
		this.from = from;
		this.to = to;
		paybuf = null;
		payload = reason;
    }

    // special case when reason is an atom (i.e. most of the time)
    def initOtpMsg6(tag : Int ,from : OtpErlangPid ,to : OtpErlangPid,reason : String) {
		this.tag = tag;
		this.from = from;
		this.to = to;
		paybuf = null;
		payload = Symbol(reason);
    }

    // other message types (link, unlink)
    def initOtpMsg7(tag : Int,from : OtpErlangPid,to : OtpErlangPid ) {
		// convert TT-tags to equiv non-TT versions
	    var tmp = tag
		if (tmp > 10) {
		    tmp -= 10
		}
	
		this.tag = tmp
		this.from = from
		this.to = to
    }

    /**
     * Get the payload from this message without deserializing it.
     * 
     * @return the serialized Erlang term contained in this message.
     * 
     */
    def getMsgBuf() : OtpInputStream = {
    	paybuf
    }

    
    def ntype() : Int ={
    	tag
    }

    
    def getMsg(): Any ={
		if (payload == null) {
		    payload = paybuf.read_any();
		}
		payload
    }

    
    def getRecipientName() : String ={
    	toName
    }

    /**
     * <p>
     * Get the Pid of the recipient for this message, if it is a sendTag
     */
    def  getRecipientPid() : OtpErlangPid ={
    	to
    }

    /**
     * <p>
     * Get the name of the recipient for this message, if it is a regSendTag
     * message.
     * </p>
     * 
     * <p>
     * Messages are sent to Pids or names. If this message was sent to a name
     * then the name is returned by this method.
     * </p>
     * 
     * @return the Pid of the recipient, or null if the recipient was in fact a
     *         name.
     */
    def  getRecipient() : Object = {
		if (toName != null) {
		    return toName
		}
		return to
    }

    /**
     * <p>
     * Get the Pid of the sender of this message.
     * </p>
     * 
     * <p>
     * For messages sent to names, the Pid of the sender is included with the
     * message. The sender Pid is also available for link, unlink and exit
     * messages. It is not available for sendTag messages sent to Pids.
     * </p>
     * 
     * @return the Pid of the sender, or null if it was not available.
     */
    def getSenderPid() : OtpErlangPid ={
    	from
    }
}
