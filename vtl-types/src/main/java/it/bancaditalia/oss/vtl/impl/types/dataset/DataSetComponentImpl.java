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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIME_PERIODDS;

import java.util.HashMap;
import java.util.Map;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDurationDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireNumberDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class DataSetComponentImpl<R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements DataSetComponent<R, S, D>
{
	private static final long serialVersionUID = 1L;

	public static final DataSetComponent<Measure, EntireIntegerDomainSubset, IntegerDomain> INT_VAR = new DataSetComponentImpl<>(VTLAliasImpl.of("int_var"), Measure.class, INTEGERDS);
	public static final DataSetComponent<Measure, EntireNumberDomainSubset, NumberDomain> NUM_VAR = new DataSetComponentImpl<>(VTLAliasImpl.of("num_var"), Measure.class, NUMBERDS);
	public static final DataSetComponent<Measure, EntireStringDomainSubset, StringDomain> STRING_VAR = new DataSetComponentImpl<>(VTLAliasImpl.of("string_var"), Measure.class, STRINGDS);
	public static final DataSetComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> BOOL_VAR = new DataSetComponentImpl<>(VTLAliasImpl.of("bool_var"), Measure.class, BOOLEANDS);
	public static final DataSetComponent<Measure, EntireDurationDomainSubset, DurationDomain> DURATION_VAR = new DataSetComponentImpl<>(VTLAliasImpl.of("duration_var"), Measure.class, DURATIONDS);
	public static final DataSetComponent<Measure, EntireTimePeriodDomainSubset, TimePeriodDomain> PERIOD_VAR = new DataSetComponentImpl<>(VTLAliasImpl.of("period_var"), Measure.class, TIME_PERIODDS);
	
	private static final Map<ValueDomainSubset<?, ?>, DataSetComponent<Measure, ?, ?>> DOMAIN_TO_VAR = new HashMap<>();
	
	static
	{
		DOMAIN_TO_VAR.put(INTEGERDS, INT_VAR);
		DOMAIN_TO_VAR.put(NUMBERDS, NUM_VAR);
		DOMAIN_TO_VAR.put(STRINGDS, STRING_VAR);
		DOMAIN_TO_VAR.put(BOOLEANDS, BOOL_VAR);
		DOMAIN_TO_VAR.put(DURATIONDS, DURATION_VAR);
		DOMAIN_TO_VAR.put(TIME_PERIODDS, PERIOD_VAR);
	}

	public static DataSetComponent<Measure, ?, ?> getDefaultMeasure(ValueDomainSubset<?, ?> domain)
	{
		VTLAliasImpl domAlias = (VTLAliasImpl) domain.getAlias();
		return DOMAIN_TO_VAR.getOrDefault(domain, DataSetComponentImpl.of(VTLAliasImpl.of(domAlias.isQuoted(), domAlias.getName() + "_var"), domain, Measure.class));
	}

	private final VTLAlias alias;
	private final Class<R> role;
	private final S domain;
	private final int hashCode;

	public DataSetComponentImpl(DataStructureComponent<R> template, S domain)
	{
		this(template.getAlias(), template.getRole(), domain);
	}

	public DataSetComponentImpl(VTLAlias alias, Class<R> role, S domain)
	{
		this.alias = alias;
		this.role = role;
		this.domain = domain;
		this.hashCode = defaultHashCode();
	}
	
	@SuppressWarnings("unchecked")
	public static <R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> DataSetComponent<R, ?, ?> of(VTLAlias alias, ValueDomainSubset<?, ?> domain, Class<? extends R> role)
	{
		return new DataSetComponentImpl<>(alias, (Class<R>) role, (S) domain);
	}

	@Override
	public VTLAlias getAlias()
	{
		return alias;
	}
	
	@Override
	public ValueDomainSubset<S, D> getDomain()
	{
		return domain;
	}
	
	@Override
	public DataSetComponent<R, S, D> getRenamed(VTLAlias newAlias)
	{
		return new DataSetComponentImpl<>(newAlias, role, domain);
	}

	@Override
	public Class<R> getRole()
	{
		return role;
	}
	
	@Override
	public <S1 extends ValueDomainSubset<S1, D1>, D1 extends ValueDomain> DataSetComponent<R, S1, D1> getCasted(S1 domain)
	{
		if (domain.isAssignableFrom(this.domain))
			return new DataSetComponentImpl<>(alias, role, domain);
		else
			throw new VTLCastException(domain, this.domain);
	}

	@Override
	public int hashCode()
	{
		return hashCode;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof DataSetComponent))
			return false;
		
		DataSetComponent<?, ?, ?> other = (DataSetComponent<?, ?, ?>) obj;
		return role == other.getRole() && alias.equals(other.getAlias()) && domain.equals(other.getDomain());
	}

	@Override
	public String toString()
	{
		return (is(Identifier.class) ? "$" : "") + (is(Attribute.class) ? is(ViralAttribute.class) ? "@@" : "@" : "") + getAlias() + "[" + getDomain() + "]";	
	}
}
