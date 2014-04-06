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

package me.schiz.jmeter.ring.udp.sampler;

import io.netty.util.HashedWheelTimer;
import me.schiz.jmeter.ring.udp.Ring;
import me.schiz.jmeter.ring.udp.Token;
import me.schiz.jmeter.ring.udp.config.UDPRingSourceElement;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

public class UDPRingSampler extends AbstractSampler {
	private static final Logger log = LoggingManager.getLoggerForClass();

	public static final String SOURCE = "UDPRingSampler.source";
	public static final String REQUEST = "UDPRingSampler.request";
	public static final String HEX = "UDPRingSampler.hex";

	public static final ThreadLocal<ByteBuffer> tlRequest = new ThreadLocal<ByteBuffer>();
	private static final ThreadLocal<ByteBuffer> tlBuffer = new ThreadLocal<ByteBuffer>();
	private static final ThreadLocal<ConcurrentLinkedQueue<SampleResult>> tlQueue = new ThreadLocal<ConcurrentLinkedQueue<SampleResult>>();

	public void setSource(String source) {
		setProperty(SOURCE, source);
	}
	public String getSource() {
		return getPropertyAsString(SOURCE);
	}
	public void setRequest(String request) {
		setProperty(REQUEST, request);
	}
	public String getRequest() {
		return getPropertyAsString(REQUEST);
	}
	public void setHex(boolean hex) {
		setProperty(HEX, hex);
	}
	public boolean isHex() {
		return getPropertyAsBoolean(HEX);
	}

	public UDPRingSampler() {
	}

	@Override
	public SampleResult sample(Entry entry) {
		boolean idling = false;
		SampleResult newSampleResult = new SampleResult();
		newSampleResult.setSampleLabel(getName());

		ConcurrentLinkedQueue<SampleResult> queue = tlQueue.get();
		if(queue == null) {
			queue = new ConcurrentLinkedQueue<SampleResult>();
			tlQueue.set(queue);
		}

		Ring ring = UDPRingSourceElement.get(getSource());
		Token t;
		int tid = -1;
		byte[] request_in_bytes = new byte[0];

		ByteBuffer request = tlRequest.get();
		if(request == null) {
			request = tlBuffer.get();
			if(request == null) {
				request = ByteBuffer.allocateDirect(8*1024*1024);
				tlBuffer.set(request);
			}
			request.clear();

			if(isHex()) {
				try {
					request_in_bytes = Hex.decodeHex(getRequest().toCharArray());
				} catch (DecoderException e) {
					log.error("can't decode request", e);
					idling = true;
				}
			} else {
				request_in_bytes = getRequest().getBytes();
			}
			request.put(request_in_bytes);
		}
		if(!idling) {
			try{
				request.flip();
				while(tid == -1) {
					tid = ring.acquire();
				}
				t = ring.get(tid);
				t.lock.lock();
				if(isHex())	t.ishex = true;
				newSampleResult.sampleStart();
				try {
					//t.socketChannel.write(request);
					t.sampleResult = newSampleResult;
					t.queue = queue;
					ring.write(tid, request);
					request.clear();
					newSampleResult.setSuccessful(true);
				} catch (IOException e) {
					newSampleResult.setSuccessful(false);
					ring.reset(tid);
					log.warn("IOException", e);
				} finally {
					t.lock.unlock();
				}

			} catch (Exception e) {
				log.error("Exception", e);
				newSampleResult.setSuccessful(false);
				newSampleResult.setResponseCode(e.getClass().getName());
				while(!queue.offer(newSampleResult)){}
				if(tid != -1)	ring.reset(tid);
			} finally {
				newSampleResult.setRequestHeaders(getRequest());
			}
		}
		SampleResult sampleResult = queue.poll();
		return sampleResult;
	}
}
