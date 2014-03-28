package com.qifun.otp.erlang



import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

object GenericQueue {
	def apply(i : Int) = {
		val queue = new GenericQueue
		queue.init
		queue.initState
		queue
	}
}

/**
 * 消息队列
 */
class GenericQueue {

	val open = 0
	val closing = 1
	val closed = 2

	var status: Int = 0
	var count = 0

	val queue = new LinkedBlockingQueue[Any]()
	/**
	 * 初始化队列参数
	 * Add an object to the tail of the queue.
	 *
	 * @param o
	 *                Object to insert in the queue
	 *
	 *               def put(o : Object ):Unit = synchronized
	 */
	def put(o: Any) {
		if(o != null)
			queue.offer(o)
	}
	
	
	def init() {
		count = 0
	}

	def initState {
		status = open
	}

	/** Clear a queue */
	def flush() {
		queue.clear()
	}

	def close() {
		status = closing
	}

	

	/**
	 * Retrieve an object from the head of the queue, or block until one
	 * arrives.
	 *
	 * @return The object at the head of the queue.
	 * def get() : Object = synchronized
	 */
	def get(): Any = {
		var o: Any = queue.take()
		o
	}

	/**
	 * Retrieve an object from the head of the queue, blocking until one arrives
	 * or until timeout occurs.
	 *
	 * @param timeout
	 *                Maximum time to block on queue, in ms. Use 0 to poll the
	 *                queue.
	 *
	 * @return The object at the head of the queue, or null if none arrived in
	 *         time.
	 *
	 *         def get(timeout : Long) : Object = synchronized{}
	 */

	def get(timeout: Long): Any = {
		queue.poll(timeout, TimeUnit.MILLISECONDS)
	}


	/**
	 * 获取当前队列大小
	 */
	def getCount(): Int = {
		queue.size()
	}
}
