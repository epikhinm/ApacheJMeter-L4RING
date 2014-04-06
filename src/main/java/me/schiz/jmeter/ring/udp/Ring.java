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

import com.google.common.collect.MapMaker;
import io.netty.util.HashedWheelTimer;
import me.schiz.ringpool.BinaryRingPool;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;

import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.*;
import java.util.regex.PatternSyntaxException;

public class Ring {
	private static final Logger log = LoggingManager.getLoggerForClass();

	private int capacity;
	private int selectorsCount;

	private BinaryRingPool<Token>	ring;
	private Thread[] threads;
	private EventLoopRunnable[] eventLoopRunnables;
	private String[] addrs;

	private int responseTimeout = 750;
	private int bufferSize = 4096;

	private ConcurrentMap<DatagramChannel, Token> weakSocketToTokenMap;
	private HashedWheelTimer hashedWheelTimer;
	private ScheduledExecutorService schedEx;
	private final static int THREADS = Runtime.getRuntime().availableProcessors() / 4 + 1;

	public Ring(int capacity, int selectorsCount) {
		this.capacity = capacity;
		this.selectorsCount = selectorsCount;

		ring = new BinaryRingPool<>(capacity);
		for(int i = 0; i<capacity; ++i) {
			Token t = new Token();
			if(!ring.put(t)) {
				log.error("can't put token into ring");
			}
		}
		this.addrs = null;

		this.threads = new Thread[selectorsCount];
		//this.eventLoopRunnables = new EventLoopRunnable[selectorsCount];
		schedEx = Executors.newScheduledThreadPool(1);
	}

	public Ring setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public Ring setResponseTimeout(int timeout) {
		this.responseTimeout = timeout;
		return this;
	}

	public int getResponseTimeout() {
		return responseTimeout;
	}

	public Ring setRemoteAddresses(String addresses) {
		if(addresses == null) {
			log.error("empty address");
		}
		else if(addresses.isEmpty()) {
			log.error("empty address");
		}
		else {
			try{
				this.addrs = addresses.split(" ");
			} catch (PatternSyntaxException pse) {
				log.error("failed split \"" + addresses + "\" by space", pse);
				this.addrs = null;
			}
		}
		return this;
	}

	private void setSocketOptions(DatagramChannel datagramChannel) throws IOException {
		datagramChannel.configureBlocking(false);
		datagramChannel.setOption(StandardSocketOptions.SO_SNDBUF, getBufferSize());
		datagramChannel.setOption(StandardSocketOptions.SO_RCVBUF, getBufferSize());
		datagramChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
	}

	public Ring init() {
		hashedWheelTimer = new HashedWheelTimer();
		eventLoopRunnables = new EventLoopRunnable[THREADS];
		for(int i = 0; i<selectorsCount;++i) {
			eventLoopRunnables[i] = new EventLoopRunnable(this);
			threads[i] = new Thread(eventLoopRunnables[i]);
			threads[i].setDaemon(true);
			threads[i].setName("EventLoopThread#" + i);
		}

		weakSocketToTokenMap = new MapMaker()
				.concurrencyLevel(Runtime.getRuntime().availableProcessors())
				.initialCapacity(capacity)
				.softKeys()
				.makeMap();

		for(int i=0;i<capacity;i++) {
			String[] addr;
			String host = "localhost";
			int port;
			try{
				addr = addrs[i%addrs.length].split(":");
				host = addr[0];
				port = Integer.parseInt(addr[1]);
			} catch (PatternSyntaxException | NumberFormatException e) {
				log.error("bad address \"" + addrs[i%addrs.length] + "\"", e);
				return this;
			}
			try {
				Token t = ring.get(i);
				t.id = i;
				t.targetAddress = new InetSocketAddress(host, port);
				setSocketOptions(t.datagramChannel);
				try {
					eventLoopRunnables[i%selectorsCount].register(t.datagramChannel, SelectionKey.OP_READ);
				} catch (InterruptedException e) {
					log.error("InterruptedException when register SocketChannel", e);
					break;
				}
				ring.get(i).datagramChannel.connect(ring.get(i).targetAddress);
				weakSocketToTokenMap.putIfAbsent(t.datagramChannel, t);
			} catch (IOException e) {
				log.error("IOException ", e);
			}
		}

		for(int i =0;i<selectorsCount;i++)
			threads[i].start();


		//schedEx.scheduleWithFixedDelay(new RingInfoRunnable(this), 0, 1000, TimeUnit.MILLISECONDS);

		return this;
	}

	public Ring reset(int token_id) {
		log.warn("reset token #" + token_id);

		Token t = ring.get(token_id);
		try {
			//t.datagramChannel.disconnect();
			//t.datagramChannel.close();
			//t.datagramChannel = DatagramChannel.open();
			setSocketOptions(t.datagramChannel);
			eventLoopRunnables[token_id%selectorsCount].register(t.datagramChannel, SelectionKey.OP_READ);
			if(!t.datagramChannel.isConnected())	t.datagramChannel.connect(t.targetAddress);
			weakSocketToTokenMap.putIfAbsent(t.datagramChannel, t);
			ring.release(t.id);
		} catch (InterruptedException e) {
			log.error("InterruptedException when register DatagramChannel", e);
		} catch (IOException e) {
			log.error("IOException ", e);
		}
		return this;
	}

	public Ring destroy() {
		schedEx.shutdown();

		for(int i=0; i < selectorsCount ; ++i) {
			eventLoopRunnables[i].stop();
		}

		for(int i = 0 ; i < capacity ; ++i) {
			if(ring == null)	break;
			if(ring.get(i) == null)	continue;
			ring.get(i).destroy();
		}

		ring = new BinaryRingPool<>(capacity);
		this.weakSocketToTokenMap.clear();

		return this;
	}

	public Token get(int id) {
		return ring.get(id);
	}

	public Token get(DatagramChannel dc) {
		Token t = weakSocketToTokenMap.get(dc);
		if(t == null) {
			log.warn("not found SocketChannel at weakMap");
			for(int i=0;i<capacity;i++) {
				if(ring.get(i).datagramChannel == dc) {
					t = ring.get(i);
					weakSocketToTokenMap.putIfAbsent(dc, t);
					break;
				}
			}
			if(t == null)	log.error("not found SocketChannel at ring");
		}
		return t;
	}

	public int acquire() {
		int i = -1, loopCount = 0;
		boolean slow_acquire_run = true;
		try {
			//Fast acquire
			for(; loopCount < 2; loopCount++) {
				i = ring.acquire();
				if(i != -1) break;
			}
		} catch (Exception e) {
			log.error("Exception", e);
			return -1;
		}
		return i;
	}

	public Ring release(int id) {
		ring.release(id);
		return this;
	}

	public Ring write(int id, ByteBuffer buffer) throws IOException {
   		Token t = ring.get(id);
		t.timeout = hashedWheelTimer.newTimeout(new TimeoutTask(this, id, "response timeout"),
				t.responseTimeout, TimeUnit.MILLISECONDS);

		t.datagramChannel.send(buffer, t.targetAddress);
		while(buffer.hasRemaining()){
			t.datagramChannel.send(buffer, t.targetAddress);
		}
		return this;
	}

	public BinaryRingPool.Stats getStats() {
		return ring.getStats();
	}

}
