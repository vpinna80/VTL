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
package it.bancaditalia.oss.vtl.impl.types.statement;

import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.TRUE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet.DataPointRule;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DataPointRuleImpl implements DataPointRule, Serializable
{
	private final VTLAlias ruleId;
	private final Transformation when;
	private final Transformation then;
	private final ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> errorcode;
	private final ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> errorlevel;

	public DataPointRuleImpl(VTLAlias ruleId, Transformation when, Transformation then, 
			ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> errorcode, 
			ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> errorlevel)
	{
		this.ruleId = ruleId;
		this.when = when;
		this.then = then;
		this.errorcode = errorcode != null ? errorcode : NullValue.instance(STRINGDS);
		this.errorlevel = errorlevel != null ? errorlevel : NullValue.instance(INTEGERDS);
	}
	
	public DataPointRuleImpl getRenamed(VTLAlias newAlias)
	{
		return new DataPointRuleImpl(newAlias, when, then, errorcode, errorlevel);
	}

	private static final long serialVersionUID = 1L;

	@Override
	public BooleanValue<?> eval(DataPoint dp, TransformationScheme scheme)
	{
		BooleanValue<?> pre = when != null ? (BooleanValue<?>) when.eval(scheme) : null;
		
		return pre != TRUE  ? TRUE : (BooleanValue<?>) then.eval(scheme);
	}

	@Override
	public VTLAlias getRuleId()
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