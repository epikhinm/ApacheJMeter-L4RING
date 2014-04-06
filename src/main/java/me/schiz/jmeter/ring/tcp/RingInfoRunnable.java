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

import me.schiz.ringpool.BinaryRingPool;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class RingInfoRunnable implements Runnable {
	private static final Logger log = LoggingManager.getLoggerForClass();
	private Ring ring;

	public RingInfoRunnable(Ring r) {
		this.ring = r;
	}

	@Override
	public void run() {
		BinaryRingPool.Stats stats = ring.getStats();
		StringBuilder sb = new StringBuilder();

		sb.append("ring\tfree:\t");
		sb.append(stats.free_objects);
		sb.append(" busy:\t");
		sb.append(stats.busy_objects);
		sb.append("\tnull:\t");
		sb.append(stats.null_objects);
		sb.append("\tnot_null:\t");
		sb.append(stats.notnull_objects);

		log.info(sb.toString());
	}
}
