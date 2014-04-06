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

import com.google.common.collect.MapMaker;
import io.netty.util.HashedWheelTimer;
import me.schiz.ringpool.BinaryRingPool;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.PatternSyntaxException;

public class Ring {
	private static final Logger log = LoggingManager.getLoggerForClass();

	private int socketsCount;
	private int selectorsCount;

	private BinaryRingPool<Token>	ring;
	private Selector[] selectors;
	private Thread[] threads;
	private EventLoopRunnable[] eventLoopRunnables;
	private String[] addrs;
	private ConcurrentMap<SocketChannel, Token> weakSocketToTokenMap;
	//private HashedWheelTimer hashedWheelTimer;
	private HashedWheelTimer[] hashedWheelTimers;

	private int connectTimeout = 750;
	private int socketTimeout = 750;

	private int bufferSize = 4096;

	private ScheduledExecutorService schedEx;
	private final static int THREADS = Runtime.getRuntime().availableProcessors();
	private final static int ACQUIRE_SLEEP = 1;
	private Object[] acqMonitors;
	private AtomicInteger waitersCount;

	public Ring(int socketsCount, int selectorsCount) {
		this.socketsCount = socketsCount;
		this.selectorsCount = selectorsCount;

		ring = new BinaryRingPool<>(this.socketsCount);
		for(int i = 0; i<socketsCount; ++i) {
			Token t = new Token();
			if(!ring.put(t)) {
				log.error("can't put token into ring");
			}
		}
		this.addrs = null;

		this.threads = new Thread[selectorsCount];
		this.eventLoopRunnables = new EventLoopRunnable[selectorsCount];
		this.selectors = new Selector[selectorsCount];
		this.hashedWheelTimers = new HashedWheelTimer[THREADS/4+1];
		this.acqMonitors = new Object[THREADS];
		this.waitersCount = new AtomicInteger(0);

		for(int i=0;i<THREADS;i++) {
			acqMonitors[i] = new Object();
		}

		schedEx = Executors.newScheduledThreadPool(1);
	}

	public Ring setConnectiontimeout(int connectiontimeout) {
		this.connectTimeout = connectiontimeout;
		return this;
	}

	public Ring setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
		return this;
	}

	public Ring setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public int getBufferSize() {
		return bufferSize;
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

	private void setSocketOptions(SocketChannel socketChannel) throws IOException {
		socketChannel.configureBlocking(false);
		socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, getBufferSize());
		socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, getBufferSize());
		socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
		socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
		socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
	}

	public Ring init() {
		//hashedWheelTimer = new HashedWheelTimer();
		//hashedWheelTimer.start();
		for(int i=0;i<hashedWheelTimers.length;i++) {
			hashedWheelTimers[i] = new HashedWheelTimer();
			hashedWheelTimers[i].start();
		}

		for(int i=0;i<selectorsCount;i++) {
			try {
				selectors[i] = Selector.open();
			} catch (IOException e) {
				log.error("can't open selector ", e);
			}
		}

		for(int i = 0; i<selectorsCount;++i) {
			eventLoopRunnables[i] = new EventLoopRunnable(this, selectors[i]);
			threads[i] = new Thread(eventLoopRunnables[i]);
			threads[i].setDaemon(true);
			threads[i].setName("EventLoopThread#" + i);
			threads[i].start();
		}

		weakSocketToTokenMap = new MapMaker()
				.concurrencyLevel(Runtime.getRuntime().availableProcessors())
				.initialCapacity(socketsCount)
				.softKeys()
				.makeMap();

		for(int i=0;i<socketsCount;i++) {
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
				setSocketOptions(t.socketChannel);
				try {
					eventLoopRunnables[i%selectorsCount].register(t.socketChannel,
							SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
				} catch (InterruptedException e) {
					log.error("InterruptedException when register SocketChannel", e);
					break;
				}
				ring.get(i).connectStartTS = System.nanoTime();
				ring.get(i).socketChannel.connect(ring.get(i).targetAddress);
				ring.get(i).timeout = hashedWheelTimers[i%hashedWheelTimers.length].newTimeout(new TimeoutTask(this, i, "connect"),
						connectTimeout, TimeUnit.MILLISECONDS);
				weakSocketToTokenMap.putIfAbsent(t.socketChannel, t);
			} catch (IOException e) {
				log.error("IOException ", e);
			}
		}

		schedEx.scheduleWithFixedDelay(new RingInfoRunnable(this), 0, 1000, TimeUnit.MILLISECONDS);

		return this;
	}

	public Ring reset(int token_id, String reason) {
		log.warn("reset token #" + token_id +  " reason: " + reason);

		Token t = ring.get(token_id);
		try {
			t.isPrepared = false;
			t.socketChannel.close();
			t.socketChannel = SocketChannel.open();
			t.socketChannel.configureBlocking(false);
			setSocketOptions(t.socketChannel);
			eventLoopRunnables[token_id%selectorsCount].register(t.socketChannel,
					SelectionKey.OP_CONNECT | SelectionKey.OP_READ);

			t.connectStartTS = System.nanoTime();
			t.timeout = hashedWheelTimers[t.id%selectorsCount].newTimeout(new TimeoutTask(this, token_id, "connect timeout"),
					connectTimeout, TimeUnit.MILLISECONDS);
			weakSocketToTokenMap.putIfAbsent(t.socketChannel, t);
			t.socketChannel.connect(t.targetAddress);
			ring.release(t.id);
		} catch (InterruptedException e) {
			log.error("InterruptedException when register SocketChannel", e);
			log.error("token loss " + token_id);
		} catch (IOException e) {
			log.error("IOException ", e);
			log.error("token loss " + token_id);
		}
		return this;
	}

	public Ring write(int id, ByteBuffer buffer) throws IOException {
		Token t = ring.get(id);
		t.timeout = hashedWheelTimers[id%hashedWheelTimers.length].newTimeout(new TimeoutTask(this, id, "response timeout"),
				this.socketTimeout, TimeUnit.MILLISECONDS);

		t.socketChannel.write(buffer);
		while(buffer.hasRemaining()){
			t.socketChannel.write(buffer);
		}
		return this;
	}

	public Ring timeout(int id, String reason) {
		Token t = ring.get(id);
		try {
			eventLoopRunnables[t.id%selectorsCount].timeout(t, reason);
		} catch (InterruptedException e) {
			log.warn("InterruptedException", e);
			reset(id, "interruptedException in Ring.timeout() method");
		}
		return this;
	}


	public Ring destroy() {
		schedEx.shutdown();

		for(int i=0;i<hashedWheelTimers.length;i++)
			hashedWheelTimers[i].stop();

		for(int i=0; i < selectorsCount ; ++i) {
			try {
				selectors[i].close();
			} catch (IOException e) {
				log.error("can't close selector #" + i, e);
			}
		}

		for(int i = 0 ; i < socketsCount ; ++i) {
		    if(ring == null)	break;
			if(ring.get(i) != null)	ring.get(i).destroy();
		}

		ring = new BinaryRingPool<>(socketsCount);

		this.weakSocketToTokenMap.clear();

		return this;
	}

	public Token get(int id) {
		return ring.get(id);
	}

	public Token get(SocketChannel socketChannel) {
		Token t = weakSocketToTokenMap.get(socketChannel);
		if(t == null) {
			log.warn("not found SocketChannel at weakMap");
			for(int i=0;i<socketsCount;i++) {
				if(ring.get(i).socketChannel == socketChannel) {
					t = ring.get(i);
					weakSocketToTokenMap.putIfAbsent(socketChannel, t);
					break;
				}
			}
			if(t == null)	log.error("not found SocketChannel at ring");
		}
		return t;
	}

	public int acquire() {
		int i = -1, loopCount = 0;
		try {
			//Fast acquire
			for(; loopCount < 2; loopCount++) {
				i = ring.acquire();
				if(i != -1) break;
			}
			//Slow acquire
			//TODO slow acquire
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

	public BinaryRingPool.Stats getStats() {
		return ring.getStats();
	}

}
