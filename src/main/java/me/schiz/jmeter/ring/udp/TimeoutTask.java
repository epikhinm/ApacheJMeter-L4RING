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

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.util.Queue;

public class TimeoutTask implements TimerTask {
	private static final Logger log = LoggingManager.getLoggerForClass();
	private Ring ring;
	private int id;
	private String reason;

	private final static String ERROR_502 = "502";

	public TimeoutTask(Ring ring, int token_id, String reason) {
		this.ring = ring;
		this.id = token_id;
		this.reason = reason;
	}

	public boolean cancel() {
		return true;
	}

	@Override
	public void run(Timeout timeout) throws Exception {
		if(timeout.isExpired() && !timeout.isCancelled()) {
			ring.get(id).lock.lock();
			log.error("Timeout token #" + id + ". Reason: " + reason);
			//ring.reset(id);
			Token t = ring.get(id);
			try{
				SampleResult sr = t.sampleResult;
				Queue queue = t.queue;
				t.sampleResult = null;
				t.queue = null;
				sr.setSuccessful(false);
				sr.setResponseCode(ERROR_502);
				sr.sampleEnd();
				while(!queue.offer(sr)) {}
			} catch (NullPointerException npe) {

			}
			ring.reset(id);
			ring.get(id).lock.unlock();
		}
	}
}
