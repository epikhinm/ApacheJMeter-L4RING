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

import me.schiz.ringpool.BinaryRingPool;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RingAmmo {
	private static final Logger log = LoggingManager.getLoggerForClass();
	private Thread[] readers;
	private RingAmmoRunnable[] runnables;
	private String[] files;
	private BinaryRingPool<String>	pool;
	private AtomicInteger size;
	private AtomicInteger sleepCounter;
	private int capacity;
	private int bufferSize;
	private static final int NOTIFY_THRESHOLD = 20; //if size / capacity ~20%, than every take() notify ReadThreads

	public RingAmmo(String name, String files, int capacity, int bufferSize) throws FileNotFoundException {
		if(files.indexOf(";") != -1) {
			this.files = files.split(";");
		} else {
			this.files = new String[1];
			this.files[0] = files;
		}
		this.readers = new Thread[this.files.length];
		this.runnables = new RingAmmoRunnable[this.files.length];
		this.capacity = capacity;
		this.pool = new BinaryRingPool<String>(capacity);
		this.size = new AtomicInteger(0);
		this.sleepCounter = new AtomicInteger(0);
		this.bufferSize = bufferSize;
		for(int i=0;i<this.readers.length;i++) {
			this.runnables[i] = new RingAmmoRunnable(this.files[i], pool, size, sleepCounter, bufferSize);
			this.readers[i] = new Thread(this.runnables[i]);
			this.readers[i].setDaemon(true);
			this.readers[i].setName("AmmoReaderThread-" + name + "#" + i);
			this.readers[i].start();
		}
	}

	public void start() {
		for(int i=0;i<this.readers.length;i++) {
			this.readers[i].start();
		}
	}

	public void end() {
		for(int i=0;i<this.runnables.length;i++) {
			this.runnables[i].end();
		}
	}

	public String take() {
		int acq=-1;
		while(acq==-1) {
			acq = pool.acquire();
		}
		int sz = size.decrementAndGet();
		if(sz*100/capacity <= NOTIFY_THRESHOLD && sleepCounter.get() > 0) {
			synchronized (size) {
				size.notify();
			}
		}
		String result = pool.get(acq);
		pool.destroy(acq);
		pool.release(acq);
		//hotfix:)
		if(result == null)	return this.take();
		return result;
	}

	class RingAmmoRunnable implements Runnable {
	   	private String file;
		private volatile boolean run;
		private BufferedReader reader;
		private BinaryRingPool<String> ring;
		private AtomicInteger size;
		private AtomicInteger sleepCounter;
		private int bufferSize;

		private final static int SLEEP = 10; // 10ms

		public RingAmmoRunnable(String file, BinaryRingPool<String> ring,
								AtomicInteger size, AtomicInteger sleepCounter,
								int bufferSize) throws FileNotFoundException {
			this.file = file;
			reader = new BufferedReader( new FileReader(file));
			run = true;
			this.ring = ring;
			this.size = size;
			this.sleepCounter = sleepCounter;
			this.bufferSize = bufferSize;
		}

		private void reloadInputStream() throws IOException {
			reader.close();
			reader =  new BufferedReader(new FileReader(file));
		}

		public void end() {
			this.run = false;
		}

		private void put(String buffer) {
			//double put
			boolean success = false;
			for(int i=0;i<2 & run;i++) {
				if(ring.put(buffer)) {
					success = true;
					break;
				}
			}
			if(success) {
				size.incrementAndGet();
			} else {
				//wait-notify put
				while(!success & run) {
					synchronized (size) {
						try {
							sleepCounter.incrementAndGet();
							size.wait(SLEEP);
						} catch (InterruptedException e) {
							log.warn("Can't put ammo", e);
						}
					}
					if(ring.put(buffer)) {
						sleepCounter.decrementAndGet();
						size.incrementAndGet();
						success = true;
					}
				}
			}
		}


		@Override
		public void run() {
			log.info(Thread.currentThread().getName() + " started");
			char [] buffer = new char[bufferSize];

			long reads = 0, in = 0;
			long read_thrpt = 0, inbound = 0;
			long lastTime = System.currentTimeMillis(), currrentTime;
			try{
				while(run) {
					String line = reader.readLine();
					if(line == null) {
						reloadInputStream();
						continue;
					}
					int splitter = line.indexOf(" "), length;
					if(splitter != -1) {
						length = Integer.parseInt(line.substring(0, splitter));
					} else {
						length = Integer.parseInt(line);
					}
					int read = reader.read(buffer, 0, length);
					if(read == length) {
						//while(!ring.put(String.valueOf(buffer).getBytes()) && run) {}
						put(String.valueOf(buffer));
						reads++;
						in += length;
					} else {
						reloadInputStream();
					}
					currrentTime = System.currentTimeMillis();
					if(currrentTime - lastTime >= 1000L) {
						read_thrpt = reads * 1000L / (currrentTime - lastTime);
						inbound = in * 1000L / (currrentTime - lastTime);
						inbound = inbound / 1024L;
						reads = 0;
						in = 0;
						lastTime = currrentTime;
						log.info(Thread.currentThread().getName() + "\tthrpt:\t" + read_thrpt +
								" ops\tbndwdth:\t" + inbound + " KBps");
					}

				}
				reader.close();
			} catch (IOException e) {
				log.error("IOException ", e);
			}
			log.info(Thread.currentThread().getName() + " stopped");
		}

	}

}
