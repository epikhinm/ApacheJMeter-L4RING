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

package me.schiz.jmeter.ring.udp;

import org.apache.commons.codec.binary.Hex;
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
	private ArrayBlockingQueue<KeyValue> registerQueue;

	public final static int POLL_TIMEOUT = 10; //10ms
	public final static int REGS_PER_ITERATION = 256;

	public EventLoopRunnable(Ring ring) {
		this.ring = ring;
		byteBuffer = ByteBuffer.allocateDirect(ring.getBufferSize());
		registerQueue = new ArrayBlockingQueue<KeyValue>(REGS_PER_ITERATION*4);

		try {
			this.selector = Selector.open();
		} catch (IOException e) {
			log.error("Can't open selector", e);
		}
	}

	@Override
	public void run() {
		log.info("EventLoop started in " + Thread.currentThread().getName());
		int events_count, register_count;
		while (true) {
			try {
				if (!selector.isOpen()) break;
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
					DatagramChannel dc = (DatagramChannel) key.channel();

					try{
						if (key.isReadable())	readCallback(dc);
					} catch (CancelledKeyException e) {
						log.error("cancelled key exception", e);
						Token t =  ring.get(dc);
						if(t != null) {
							t.lock.lock();
							ring.reset(t.id);
							t.lock.unlock();
						}
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

	public void register(DatagramChannel dc, int ops) throws InterruptedException {
		registerQueue.put(new KeyValue(dc, ops));
		if(registerQueue.size() >= REGS_PER_ITERATION / 2)	selector.wakeup();
	}

	private void readCallback(DatagramChannel dc) throws IOException {
		Token t = ring.get(dc);
		t.lock.lock();
		try{
			t.remote = dc.receive(byteBuffer);
			//EOF
			if (t != null) {
				if(t.timeout != null)	t.timeout.cancel();
			}
			byteBuffer.flip();
			if(t.sampleResult != null) {
				t.sampleResult.sampleEnd();
				byte[] res = Charset.defaultCharset().decode(byteBuffer).toString().getBytes();
				if(t.ishex) {
					t.sampleResult.setResponseData(String.valueOf(Hex.encodeHex(res, true)).getBytes());
				} else {
					t.sampleResult.setResponseData(res);
				}

				while(!t.queue.offer(t.sampleResult)) {}
				t.sampleResult = null;
				t.queue = null;
			} else {
				log.warn("have response without request");
			}
			ring.release(t.id);
		} catch (IOException e) {
			t.sampleResult.setResponseCode(e.toString());
			t.sampleResult.setSuccessful(false);
			while(!t.queue.offer(t.sampleResult)) {}
		} finally {
			t.lock.unlock();
			byteBuffer.clear();
		}
	}

	private void registerCallback(int register_count) {
		for(int i=0;i < Math.min(register_count, REGS_PER_ITERATION); i++) {
			KeyValue kv = registerQueue.poll();
			if(kv == null)	break;
			if(kv.key instanceof DatagramChannel && kv.value instanceof Integer) {
				DatagramChannel dc = (DatagramChannel)kv.key;
				int ops = (Integer)kv.value;
				try{
					dc.register(selector, ops);
				} catch (ClosedSelectorException e) {
					log.error("Selector is closed", e);
					break;
				} catch (CancelledKeyException e) {
					log.error("", e);
				} catch (ClosedChannelException e) {
					log.warn("", e);
					Token t =  ring.get(dc);
					if(t != null)	ring.reset(t.id);
				}
			}
		}
	}

	public void stop() {
		try {
			if(selector.isOpen())	selector.close();
		} catch (IOException e) {
			log.error("can't stop selector", e);
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
