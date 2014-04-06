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

package me.schiz.jmeter.ring.tcp.config;

import me.schiz.jmeter.ring.tcp.Ring;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class TCPRingSourceElement extends ConfigTestElement
		implements TestStateListener, TestBean {
	private static final Logger log = LoggingManager.getLoggerForClass();

	protected static ConcurrentHashMap<String, Ring> rings = null;
	public static final String SOURCE = "TCPRingSourceElement.source";
	public static final String THREADS = "TCPRingSourceElement.threads";
	public static final String SOCKETS = "TCPRingSourceElement.sockets";
	public static final String ADDRESSES = "TCPRingSourceElement.addresses";
	public static final String CONNECTION_TIMEOUT = "TCPRingSourceElement.connectionTimeout";
	public static final String SOCKET_TIMEOUT = "TCPRingSourceElement.socketTimeout";
	public static final String BUFFER_SIZE = "TCPRingSourceElement.bufferSize";

	public static final String DEFAULT_SOURCE = "default";
	public static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors() / 4 + 1;
	public static final int DEFAULT_SOCKETS = DEFAULT_THREADS*8;
	public static final String DEFAULT_ADDRESSES = "localhost:8080";
	public static final int DEFAULT_CONNECTION_TIMEOUT = 1500;
	public static final int DEFAULT_SOCKET_TIMEOUT = 750;
	public static final int DEFAULT_BUFFER_SIZE = 4096;

	public void setBufferSize(String v) {
		if(v == null)	return;
		setProperty(BUFFER_SIZE, v);
	}
	public String getBufferSize() {
		return getPropertyAsString(BUFFER_SIZE);
	}
	public void setSocketTimeout(String v) {
		if(v == null)	return;
		setProperty(SOCKET_TIMEOUT, v);
	}
	public String getSocketTimeout() {
		return getPropertyAsString(SOCKET_TIMEOUT);
	}
	public void setConnectionTimeout(String v) {
		if(v == null)	return;
		setProperty(CONNECTION_TIMEOUT, v);
	}
	public String getConnectionTimeout() {
		return getPropertyAsString(CONNECTION_TIMEOUT);
	}
	public void setAddresses(String v) {
		if(v == null)	return;
		try{
			setProperty(ADDRESSES, v);
		} catch (NullPointerException npe) {
			log.error(Thread.currentThread().getStackTrace().toString(), npe);
		}
	}
	public String getAddresses() {
		return getPropertyAsString(ADDRESSES);
	}
	public void setThreads(String v) {
		if(v == null)	return;
		setProperty(THREADS, v);
	}
	public String getThreads() {
		return getPropertyAsString(THREADS);
	}
	public void setSource(String v) {
		if(v == null)	return;
		setProperty(SOURCE, v);
	}
	public String getSource() {
		return getPropertyAsString(SOURCE);
	}
	public void setSockets(String v) {
		if(v == null)	return;
		setProperty(SOCKETS, v);
	}
	public String getSockets() {
		return getPropertyAsString(SOCKETS);
	}

	public TCPRingSourceElement() {
		if(rings == null) {
			synchronized (this.getClass()) {
				if(rings == null) {
					rings = new ConcurrentHashMap<String, Ring>();
				}
			}
		}
	}

	private static int atoi(String a, int def) {
		if(a == null)	return def;
		if(a.isEmpty())	return def;
		try{
			int v = Integer.parseInt(a);
			return v;
		} catch (NumberFormatException e) {
			return def;
		}
	}

	@Override
	public void testStarted() {
		if(rings.contains(getSource()))  log.warn("TCPRing `" +  getSource() + "` already created");
		else {
			Ring r;
			synchronized (TCPRingSourceElement.class) {
				r = new Ring(
						atoi(getSockets(), DEFAULT_SOCKETS),
						atoi(getThreads(), DEFAULT_THREADS)
				);
				r.setConnectiontimeout(atoi(getConnectionTimeout(), DEFAULT_CONNECTION_TIMEOUT));
				r.setSocketTimeout(atoi(getSocketTimeout(), DEFAULT_SOCKET_TIMEOUT));
				r.setRemoteAddresses(getAddresses());
				r.setBufferSize(atoi(getBufferSize(), DEFAULT_BUFFER_SIZE));
				rings.putIfAbsent(getSource(), r);
				log.info("added new ring `" + getSource() + "`");
			}
			r.init();
		}
	}

	@Override
	public void testStarted(String s) {
		testStarted();
	}

	@Override
	public void testEnded() {
		for(String row : rings.keySet()) {
			rings.get(row).destroy();
			log.info("shutdown ring `" + row + "`");
		}
	}

	@Override
	public void testEnded(String s) {
		testEnded();
	}

	public static Ring get(String ringName) {
		return rings.get(ringName);
	}
}