/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package me.schiz.jmeter.ring.tcp;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

public class EventLoopRunnable implements Runnable {
	private static final Logger log = LoggingManager.getLoggerForClass();
	private Ring ring;
	private Selector selector;
	private ByteBuffer byteBuffer;
	private ArrayBlockingQueue<KeyValue>	registerQueue;
	private ArrayBlockingQueue<KeyValue>    timeoutQueue;

	public final static int POLL_TIMEOUT = 10; //10ms
	public final static int REGS_PER_ITERATION = 1024;

	public EventLoopRunnable(Ring ring, Selector selector) {
		this.ring = ring;
		this.selector = selector;
		byteBuffer = ByteBuffer.allocateDirect(ring.getBufferSize());
		registerQueue = new ArrayBlockingQueue<KeyValue>(REGS_PER_ITERATION*4);
		timeoutQueue = new ArrayBlockingQueue<KeyValue>(8192);
	}

	@Override
	public void run() {
		log.info("EventLoop started in " + Thread.currentThread().getName());
		int events_count, register_count, timeout_count;
		while (true) {
			try {
				if (!selector.isOpen()) break;
				timeout_count = timeoutQueue.size();
				if (timeout_count > 0)	timeoutCallback(timeout_count);
				try {
					events_count = selector.select(POLL_TIMEOUT);
				} catch(ClosedSelectorException e) {
					log.error("Selector is closed", e);
					break;
				}
				register_count = registerQueue.size();
				if (register_count > 0)	registerCallback(register_count);
				if (events_count == 0)	continue;

				Iterator<SelectionKey> it = selector.selectedKeys().iterator();
				while (it.hasNext()) {
					SelectionKey key = it.next();
					SocketChannel socketChannel = (SocketChannel) key.channel();
					try{
						if (key.isConnectable()) connectCallback(socketChannel);
						else if (key.isReadable())	readCallback(socketChannel);
					} catch (CancelledKeyException e) {
						log.error("cancelled key exception", e);
						Token t =  ring.get(socketChannel);
						if(t != null)	ring.reset(t.id, "cancelled key exception in eventloop");
					}
					it.remove();
				}
			} catch (IOException e) {
				log.error("IOException", e);
				break;
			}
		}
		byteBuffer.clear();
		log.info("EventLoop in " + Thread.currentThread().getName() + " has stopped");
	}

	public void register(SocketChannel sc, int ops) throws InterruptedException {
		registerQueue.put(new KeyValue(sc, ops));
		if(registerQueue.size() >= REGS_PER_ITERATION / 2)	selector.wakeup();
	}

	public void timeout(Token t, String reason) throws InterruptedException {
		timeoutQueue.put(new KeyValue(t, reason));
	}

	private void readCallback(SocketChannel socketChannel) {
		Token t = ring.get(socketChannel);
		try{
//			t.lock.lock();
			int read_size = socketChannel.read(byteBuffer);
			//EOF
			if (read_size == -1) {
				log.warn("closing token #"  +t.id + " " + socketChannel.getLocalAddress() + " <-> " +
						socketChannel.getRemoteAddress() + " reason: EOF");
				ring.reset(t.id, "end of file");
			}
			if (t != null) t.timeout.cancel();
			byteBuffer.flip();
			if(t.sampleResult != null) {
				t.sampleResult.sampleEnd();
				t.sampleResult.setResponseData(
						Charset.defaultCharset().decode(byteBuffer).toString().getBytes());
				if(t.queue != null)	while(!t.queue.offer(t.sampleResult))
				t.sampleResult = null;
				t.queue = null;
				t.isPrepared = true;
			}
			ring.release(t.id);
		} catch (IOException e) {
			t.sampleResult.setResponseCode(e.toString());
			t.sampleResult.setSuccessful(false);
			while(!t.queue.offer(t.sampleResult)) {}
		} finally {
//			t.lock.unlock();
			byteBuffer.clear();
		}
	}

	private void connectCallback(SocketChannel socketChannel) {
		Token t = ring.get(socketChannel);
		try{
			boolean finish = socketChannel.finishConnect();
			if (finish) {
				if (t != null) t.timeout.cancel();
				long end = System.nanoTime();
				t.isPrepared = true;
				log.info("connected token #" + t.id + " " + socketChannel.getLocalAddress() + " <-> " +
						socketChannel.getRemoteAddress() + " time: " + Token.nstoms(end - t.connectStartTS) + "ms");
				ring.release(t.id);
			} else {
				log.error("failed finishConnect on token #" + t.id);
				ring.reset(t.id, "failed finish connect");
			}
		} catch (IOException e) {
			log.warn("", e);
			ring.reset(t.id, "IOException at finishConnect");
		}
	}

	private void registerCallback(int register_count) {
		for(int i=0;i < Math.min(register_count, REGS_PER_ITERATION); i++) {
			KeyValue kv = registerQueue.poll();
			if(kv == null)	break;
			if(kv.key instanceof SocketChannel && kv.value instanceof Integer) {
				SocketChannel sc = (SocketChannel)kv.key;
				int ops = (Integer)kv.value;
				try{
					if(sc.isOpen() && !sc.isRegistered())	sc.register(selector, ops);
				} catch (ClosedSelectorException e) {
					log.error("Selector is closed", e);
					break;
				} catch (CancelledKeyException e) {
					log.error("", e);
				} catch (ClosedChannelException e) {
					log.warn("", e);
					//Token t =  ring.get(sc);
					log.error("token loss #" + ring.get(sc).id);
					//if(t != null)	ring.reset(t.id, "closedChannelException at registerCallback");
				}
			}
		}
	}

	private void timeoutCallback(int timeout_count) {
		for(int i=0; i < Math.min(timeout_count, REGS_PER_ITERATION);i++) {
			KeyValue kv = timeoutQueue.poll();
			if(kv == null)	break;
			if(kv.key instanceof Token && kv.value instanceof String) {
				Token t = (Token)kv.key;
				String reason = (String)kv.value;
				if(t.sampleResult != null && t.queue != null){
					t.sampleResult.setResponseCode("504");
					t.sampleResult.setSuccessful(false);
					t.sampleResult.setResponseData(reason.getBytes());
					while(!t.queue.offer(t.sampleResult)) {}
				}
				ring.reset(t.id, reason);
			}
		}
	}

	private class KeyValue {
		public Object key;
		public Object value;

		public KeyValue(){}
		public KeyValue(Object key, Object value) {
			this.key = key;
			this.value = value;
		}
	}
}
