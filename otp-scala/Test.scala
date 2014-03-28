package com.qifun.otp.erlang
import scala.concurrent.{ ExecutionContext, future }
object Test {

	def main(args: Array[String]): Unit = {
		//		Links(10).find(null, null)

		val node = OtpNode("realm@192.168.1.76")
		val mbox = node.createMbox("realm")
		//		import ExecutionContext.Implicits.global
		//		future {
		//			 def default(channel: String) {
		//				 println(s"No receiver for channel: [$channel")
		//			 }
		while (true) {
			mbox.receive() match {
				case msg =>
					println(msg)
//				case _ =>
			}
		}
		//		}
	}

}