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

import io.netty.util.Timeout;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

public class Token {
	private static final Logger log = LoggingManager.getLoggerForClass();

	public int id;
	public SocketChannel	socketChannel;
	public Timeout			timeout;
	public InetSocketAddress targetAddress;
	public volatile boolean isPrepared;

	public SampleResult sampleResult;
	public Queue queue;
	public long connectStartTS;

	public Token() {
		try {
			socketChannel = SocketChannel.open();
			isPrepared = false;
		} catch (IOException e) {
			log.error("can't open token " + e);
		}
	}

	public static long nstoms(long elapsedTimeNS) {
		long modulo = (elapsedTimeNS / 100000L) % 10L;
		long v = elapsedTimeNS / 1000000L;
		if(modulo >= 5L) v++;
		return v;
	}

	public void destroy() {
		try {
			socketChannel.close();
		} catch (IOException e) {
			log.error("can't close token " + e);
		}
	}
}
