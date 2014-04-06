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

package me.schiz.jmeter.ring.tcp.sampler;

import me.schiz.jmeter.ring.tcp.Ring;
import me.schiz.jmeter.ring.tcp.TimeoutTask;
import me.schiz.jmeter.ring.tcp.Token;
import me.schiz.jmeter.ring.tcp.config.TCPRingSourceElement;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class TCPRingSampler extends AbstractSampler {
	private static final Logger log = LoggingManager.getLoggerForClass();

	public static final String SOURCE = "TCPRingSampler.source";
	public static final String REQUEST = "TCPRingSampler.request";

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

	public TCPRingSampler() {
	}

	@Override
	public SampleResult sample(Entry entry) {
		SampleResult newSampleResult = new SampleResult();
		newSampleResult.setSampleLabel(getName());

		ConcurrentLinkedQueue<SampleResult> queue = tlQueue.get();
		if(queue == null) {
			queue = new ConcurrentLinkedQueue<SampleResult>();
			tlQueue.set(queue);
		}

		Ring ring = TCPRingSourceElement.get(getSource());
		Token t;
		int tid = -1;

		ByteBuffer request = tlRequest.get();
		if(request == null) {
			request = tlBuffer.get();
			if(request == null) {
				request = ByteBuffer.allocateDirect(8*1024*1024);
				tlBuffer.set(request);
			}
			request.clear();
			request.put(getRequest().getBytes());
		}
		try{
			request.flip();
			long startAcquire = System.currentTimeMillis();

			while(tid == -1) {
				tid = ring.acquire();
				if(tid != -1) {
					if(!ring.get(tid).isPrepared)	tid = -1;
				} else {
					if(System.currentTimeMillis() - startAcquire > 10L) {
						SampleResult r = queue.poll();
						if(r != null)	return r;
					}
				}

			}
			t = ring.get(tid);
//			t.lock.lock();
			t.isPrepared = false;
			newSampleResult.setSuccessful(true);
			t.sampleResult = newSampleResult;
			t.sampleResult = newSampleResult;
			t.queue = queue;
			newSampleResult.sampleStart();
			try {
				ring.write(t.id, request);
			} catch (IOException e) {
				newSampleResult.setSuccessful(false);
				log.warn("IOException", e);
				ring.reset(tid, "ioexception on write "  + e.getMessage());
			} finally {
//				t.lock.unlock();
			}

		} catch (Exception e) {
			log.error("Exception", e);
			newSampleResult.setSuccessful(false);
			newSampleResult.setResponseCode(e.getClass().getName());
			while(!queue.offer(newSampleResult)){}
			if(tid != -1)	ring.reset(tid, "exception " + e.getMessage());
		} finally {
			newSampleResult.setRequestHeaders(getRequest());
		}

		SampleResult sampleResult = queue.poll();
		return sampleResult;
	}
}
