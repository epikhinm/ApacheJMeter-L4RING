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
package me.schiz.jmeter.ring.udp.sampler.gui;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import me.schiz.jmeter.ring.udp.config.UDPRingSourceElement;
import me.schiz.jmeter.ring.udp.sampler.UDPRingSampler;
import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class UDPRingSamplerGui extends AbstractSamplerGui {
	private static final Logger log = LoggingManager.getLoggerForClass();

	private JPanel          jpGeneralPanel;
	private JTextField      tfName;
	private JTextField      tfComments;

	private JTextField      tfSource;
	private JTextArea   	taRequest;

	public UDPRingSamplerGui() {
		super();
		init();
		initFields();
	}

	public String getStaticLabel() {
		return "UDPRing Sampler";
	}

	public String getStaticLabelResource() {
		return getStaticLabel();
	}

	@Override
	public TestElement createTestElement() {
		UDPRingSampler sampler = new UDPRingSampler();
		modifyTestElement(sampler);
		return sampler;
	}

	@Override
	public String getLabelResource() {
		return this.getClass().getSimpleName();
	}

	public void configure(TestElement te) {
		super.configure(te);
		if(te instanceof UDPRingSampler) {
			UDPRingSampler sampler =(UDPRingSampler)te;
			tfName.setText(sampler.getName());
			tfComments.setText(sampler.getComment());

			tfSource.setText(sampler.getSource());
			taRequest.setText(sampler.getRequest());
		}
	}

	@Override
	public void modifyTestElement(TestElement testElement) {
		if(testElement == null) log.error("sampler is null");
		super.configureTestElement(testElement);
		if (testElement instanceof UDPRingSampler) {
			UDPRingSampler sampler = (UDPRingSampler) testElement;
			sampler.setName(tfName.getText());
			sampler.setComment(tfComments.getText());
			sampler.setSource(tfSource.getText());
			sampler.setRequest(taRequest.getText());
		}
	}
	private void initFields() {
		this.tfName.setText("UDPRing Sampler");
		this.tfComments.setText("developed by Epikhin Mikhail epikhinm@gmail.com");

		this.tfSource.setText(UDPRingSourceElement.DEFAULT_SOURCE);
		this.taRequest.setText("echo");
	}
	private void init() {
		setLayout(new BorderLayout(0, 5));
		setBorder(makeBorder());

		JPanel mainPanel = new JPanel(new GridBagLayout());


		jpGeneralPanel = new JPanel(new GridBagLayout());
		jpGeneralPanel.setAlignmentX(0);
		jpGeneralPanel.setBorder(BorderFactory.createTitledBorder(
				BorderFactory.createEtchedBorder(),
				"")); // $NON-NLS-1$

		GridBagConstraints labelConstraints = new GridBagConstraints();
		labelConstraints.anchor = GridBagConstraints.FIRST_LINE_END;

		GridBagConstraints editConstraints = new GridBagConstraints();
		editConstraints.anchor = GridBagConstraints.FIRST_LINE_START;
		editConstraints.weightx = 1.0;
		editConstraints.fill = GridBagConstraints.HORIZONTAL;

		editConstraints.insets = new java.awt.Insets(2, 0, 0, 0);
		labelConstraints.insets = new java.awt.Insets(2, 0, 0, 0);

		JPanel jpHeaderPanel = new JPanel(new GridBagLayout());
		addToPanel(jpHeaderPanel, labelConstraints, 0, 0, new JLabel("Name: ", JLabel.LEFT));
		addToPanel(jpHeaderPanel, editConstraints, 1, 0, tfName = new JTextField(20));
		addToPanel(jpHeaderPanel, labelConstraints, 0, 1, new JLabel("Comments: ", JLabel.LEFT));
		addToPanel(jpHeaderPanel, editConstraints, 1, 1, tfComments = new JTextField(20));


		addToPanel(jpGeneralPanel, labelConstraints, 0, 0, new JLabel("Source: ", JLabel.RIGHT));
		addToPanel(jpGeneralPanel, editConstraints, 1, 0, tfSource = new JTextField(32));
		addToPanel(jpGeneralPanel, labelConstraints, 0, 1, new JLabel("Request: ", JLabel.RIGHT));
		addToPanel(jpGeneralPanel, editConstraints, 1, 1, taRequest = new JTextArea());


		taRequest.setColumns(32);
		taRequest.setRows(4);
		taRequest.setLineWrap(true);
		taRequest.setWrapStyleWord(true);

		// Compilation panels
		addToPanel(mainPanel, editConstraints, 0, 0, jpHeaderPanel);
		addToPanel(mainPanel, editConstraints, 0, 1, jpGeneralPanel);

		JPanel container = new JPanel(new BorderLayout());
		container.add(makeTitlePanel(), BorderLayout.NORTH);
		container.add(mainPanel, BorderLayout.NORTH);
		add(container, BorderLayout.CENTER);
	}
	private void addToPanel(JPanel panel, GridBagConstraints constraints, int col, int row, JComponent component) {
		constraints.gridx = col;
		constraints.gridy = row;
		panel.add(component, constraints);
	}
}