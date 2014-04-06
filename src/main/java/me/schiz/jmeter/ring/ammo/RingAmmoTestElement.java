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

package me.schiz.jmeter.ring.ammo;

import me.schiz.ringpool.RingPool;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.FileNotFoundException;
import java.util.concurrent.ConcurrentHashMap;

public class RingAmmoTestElement extends ConfigTestElement
		implements TestStateListener, TestBean {
	private static final Logger log = LoggingManager.getLoggerForClass();

	private static final ConcurrentHashMap<String, RingAmmo> rings = new ConcurrentHashMap<String, RingAmmo>();
	private String ammoname;
	private String ammofile;
	private String capacity;
	private String bufferSize;

	public String getAmmoname() {
		return this.ammoname;
	}
	public void setAmmoname(String name) {
		this.ammoname = name;
	}
	public String getAmmofile() {
		return this.ammofile;
	}
	public void setAmmofile(String ammofile) {
		this.ammofile = ammofile;
	}
	public String getCapacity() {
		return this.capacity;
	}
	public void setCapacity(String capacity) {
		this.capacity = capacity;
	}
	public String getBufferSize() {
		return this.bufferSize;
	}
	public void setBufferSize(String bufferSize) {
		this.bufferSize = bufferSize;
	}

	public static String take(String cartridgeName) {
		RingAmmo ring = rings.get(cartridgeName);
		if(ring == null) {
			log.warn("Not found cartridge " + cartridgeName);
			return null;
		} else {
			return ring.take();
		}
	}

	@Override
	public void testStarted() {
		createCartridge(ammoname, ammofile, Integer.parseInt(capacity), Integer.parseInt(bufferSize));
	}

	public static void createCartridge(String name, String ammofile, int capacity, int bufferSize) {
		try {
			RingAmmo ammo = new RingAmmo(name, ammofile, capacity, bufferSize);
			rings.put(name, ammo);
			log.info("created ammo " + name + " with file " + ammofile);
		} catch (FileNotFoundException e) {
			log.error("Not found ammofile " + ammofile, e);
		}
	}

	@Override
	public void testStarted(String s) {
		testStarted();
	}

	@Override
	public void testEnded() {
		for(String ring: rings.keySet()) {
			rings.get(ring).end();
			rings.remove(ring);
		}

	}

	@Override
	public void testEnded(String s) {
		testEnded();
	}
}