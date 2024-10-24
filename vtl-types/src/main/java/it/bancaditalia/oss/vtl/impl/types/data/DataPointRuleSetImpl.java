/*
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
package it.bancaditalia.oss.vtl.impl.types.data;

import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.TRUE;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.ifNonNull;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map.Entry;

import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DataPointRuleSetImpl implements DataPointRuleSet, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final RuleSetType type;
	private final List<DataPointRule> rules;
	private final List<Entry<String, String>> vars;
	
	public static class DataPointRuleImpl implements DataPointRule, Serializable
	{
		private final String ruleId;
		private final Transformation when;
		private final Transformation then;
		private final ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> errorcode;
		private final ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> errorlevel;

		public DataPointRuleImpl(String ruleId, Transformation when, Transformation then, 
				ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> errorcode, 
				ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> errorlevel)
		{
			this.ruleId = ruleId;
			this.when = when;
			this.then = then;
			this.errorcode = errorcode;
			this.errorlevel = errorlevel;
		}

		private static final long serialVersionUID = 1L;

		@Override
		public BooleanValue<?> eval(DataPoint dp, TransformationScheme scheme)
		{
			BooleanValue<?> pre = when != null ? (BooleanValue<?>) when.eval(scheme) : null;
			
			return pre != TRUE  ? TRUE : (BooleanValue<?>) then.eval(scheme);
		}

		@Override
		public String getRuleId()
		{
			return ruleId;
		}

		@Override
		public ScalarValue<?, ?, ?, ?> getErrorCode()
		{
			return errorcode; 
		}

		@Override
		public ScalarValue<?, ?, ?, ?> getErrorLevel()
		{
			return errorlevel;
		}
		
		@Override
		public String toString()
		{
			return String.format("%s : when %s then %s errorcode %s errorlevel %s", ruleId, when, then, errorcode.get(), errorlevel.get());
		}
	}
	
	public DataPointRuleSetImpl(RuleSetType type, List<Entry<String, String>> vars, List<DataPointRule> rules)
	{
		this.type = type;
		this.vars = vars.stream().map(splitting((k, v) -> 
				new SimpleEntry<String, String>(ifNonNull(k, Variable::normalizeAlias), ifNonNull(v, Variable::normalizeAlias))
			)).collect(toList());
		this.rules = rules;
	}

	@Override
	public RuleSetType getType()
	{
		return type;
	}

	@Override
	public List<Entry<String, String>> getVars()
	{
		return vars;
	}
	
	@Override
	public List<DataPointRule> getRules()
	{
		return rules;
	}
}
