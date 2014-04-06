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

package me.schiz.jmeter.ring.ammo.functions;

import me.schiz.jmeter.ring.ammo.RingAmmoTestElement;
import org.apache.jmeter.engine.util.CompoundVariable;
import org.apache.jmeter.functions.AbstractFunction;
import org.apache.jmeter.functions.InvalidVariableException;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.samplers.Sampler;
import org.apache.jmeter.threads.JMeterVariables;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class TakeRingAmmoFunction extends AbstractFunction {
	private static final List<String> desc = new LinkedList<String>();
	private static final String KEY = "__takeRingAmmo";

	static {
		desc.add("Ammo name");
		desc.add("Name of variable in which to store the result (optional)");
	}
	private Object[] values;

	public TakeRingAmmoFunction() {
	}

	@Override
	public String execute(SampleResult previousResult, Sampler currentSampler)
			throws InvalidVariableException {
		JMeterVariables vars = getVariables();
		String cartridgeName=((CompoundVariable) values[0]).execute();
		String result = RingAmmoTestElement.take(cartridgeName);

		if (vars != null && values.length>1) {
			String varName = ((CompoundVariable) values[1]).execute().trim();
			vars.put(varName, result);
		}
		return result;
	}

	@Override
	public void setParameters(Collection<CompoundVariable> parameters) throws InvalidVariableException {
		checkMinParameterCount(parameters, 1);
		values = parameters.toArray();
	}

	@Override
	public String getReferenceKey() {
		return KEY;
	}

	public List<String> getArgumentDesc() {
		return desc;
	}
}