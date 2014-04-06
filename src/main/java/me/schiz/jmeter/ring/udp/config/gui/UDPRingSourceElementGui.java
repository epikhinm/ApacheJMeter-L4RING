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

package me.schiz.jmeter.ring.udp.config.gui;

import me.schiz.jmeter.ring.udp.config.UDPRingSourceElement;
import org.apache.jmeter.config.gui.AbstractConfigGui;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jorphan.gui.JLabeledTextField;
import org.apache.jorphan.gui.layout.VerticalLayout;

public class UDPRingSourceElementGui extends AbstractConfigGui {
	private JLabeledTextField tfSource;
	private JLabeledTextField tfThreads;
	private JLabeledTextField tfSockets;
	private JLabeledTextField tfAddresses;
	private JLabeledTextField tfResponseTimeout;
	private JLabeledTextField tfBufferSize;

	public UDPRingSourceElementGui() {
		super();
		init();
	}


	@Override
	public String getStaticLabel() {
		return "UDPRing Source Element";//$NON-NLS-1$
	}

	public String getStaticLabelResource() {
		return getStaticLabel();
	}

	public String getLabelResource() {
		return this.getClass().getName();
	}

	/**
	 * @see org.apache.jmeter.gui.JMeterGUIComponent#createTestElement()
	 */
	@Override
	public TestElement createTestElement() {
		UDPRingSourceElement config = new UDPRingSourceElement();
		config.setComment("developed by Epikhin Mikhail");
		modifyTestElement(config);
		return config;
	}

	/**
	 * Modifies a given TestElement to mirror the data in the gui components.
	 *
	 * @see org.apache.jmeter.gui.JMeterGUIComponent#modifyTestElement(TestElement)
	 */
	@Override
	public void modifyTestElement(TestElement c) {
		if (c instanceof UDPRingSourceElement) {
			UDPRingSourceElement config = (UDPRingSourceElement) c;
			config.setSource(tfSource.getText());
			config.setThreads(tfThreads.getText());
			config.setSockets(tfSockets.getText());
			config.setAddresses(tfAddresses.getText());
			config.setResponseTimeout(tfResponseTimeout.getText());
			config.setBufferSize(tfBufferSize.getText());
		}
		super.configureTestElement(c);
	}

	/**
	 * Implements JMeterGUIComponent.clearGui
	 */
	@Override
	public void clearGui() {
		super.clearGui();

		tfSource.setText(""); //$NON-NLS-1$
		tfThreads.setText(""); //$NON-NLS-1$
		tfSockets.setText(""); //$NON-NLS-1$
		tfAddresses.setText(""); //$NON-NLS-1$
		tfResponseTimeout.setText(""); //$NON-NLS-1$
		tfBufferSize.setText("");
	}

	@Override
	public void configure(TestElement element) {
		super.configure(element);
		UDPRingSourceElement config = (UDPRingSourceElement) element;
		tfSource.setText(config.getSource());
		tfThreads.setText(config.getThreads());
		tfSockets.setText(config.getSockets());
		tfAddresses.setText(config.getAddresses());
		tfResponseTimeout.setText(config.getResponseTimeout());
		tfBufferSize.setText(config.getBufferSize());
	}

	private void init() {
		setBorder(makeBorder());
		setLayout(new VerticalLayout(5, VerticalLayout.BOTH));

		tfSource = new JLabeledTextField("Source");
		tfThreads = new JLabeledTextField("Threads");
		tfSockets = new JLabeledTextField("Sockets");
		tfAddresses = new JLabeledTextField("Addresses");
		tfResponseTimeout = new JLabeledTextField("Response Timeout");
		tfBufferSize = new JLabeledTextField("Buffer Size");
		add(makeTitlePanel());
		add(tfSource);
		add(tfThreads);
		add(tfSockets);
		add(tfAddresses);
		add(tfResponseTimeout);
		add(tfBufferSize);

		tfSource.setText(UDPRingSourceElement.DEFAULT_SOURCE);
		tfThreads.setText(String.valueOf(UDPRingSourceElement.DEFAULT_THREADS));
		tfSockets.setText(String.valueOf(UDPRingSourceElement.DEFAULT_SOCKETS));
		tfAddresses.setText(UDPRingSourceElement.DEFAULT_ADDRESSES);
		tfResponseTimeout.setText(String.valueOf(UDPRingSourceElement.DEFAULT_RESPONSE_TIMEOUT));
		tfBufferSize.setText(String.valueOf(UDPRingSourceElement.BUFFER_SIZE));
	}
}