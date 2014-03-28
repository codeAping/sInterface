sInterface
==========

scala与erlang通信的组件。

下载之后，可以直接导入项目中是用，无任何依赖。
后面会继续维护其更新，完善，提高其性能。

版本1，底层网络通信采用的是阻塞式socket
后续版本会修改其底层通信机制，非阻塞

test，和demo将会在空余时间补上。






test

val node = OtpNode("realm@192.168.1.76")
val mbox = node.createMbox("realm")
while (true) {
	mbox.receive() match {
		case msg =>
			println(msg)
		case _ =>
	}
}
