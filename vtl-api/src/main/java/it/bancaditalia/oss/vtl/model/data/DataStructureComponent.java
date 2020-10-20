/**
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
package it.bancaditalia.oss.vtl.model.data;

import java.io.Serializable;
import java.util.function.UnaryOperator;

/**
 * An interface describing a component of a dataset, as defined by VTL specification.
 * 
 * @author Valentino Pinna
 *
 * @param <R> the {@link Component}
 * @param <S> the {@link ValueDomainSubset}
 * @param <D> the {@link ValueDomain}
 */
public interface DataStructureComponent<R extends Component, S extends ValueDomainSubset<D>, D extends ValueDomain> extends Serializable
{
	public Variable getVariable();
	
	public S getDomain();
	
	public Class<R> getRole(); 
	
	public default String getName()
	{
		return getVariable().getName();
	}
	
	@Override
	public boolean equals(Object obj);
	
	@Override
	public int hashCode();

	public DataStructureComponent<R, S, D> rename(String newName);

	public default DataStructureComponent<R, S, D> rename(UnaryOperator<String> nameMapper)
	{
		return rename(nameMapper.apply(getName()));
	}

	public default boolean is(Class<? extends Component> type)
	{
		return type.isAssignableFrom(getRole());
	}
	
	@SuppressWarnings("unchecked")
	public default <R2 extends Component> DataStructureComponent<R2, S, D> as(Class<R2> type)
	{
		if (is(type))
			// safe
			return (DataStructureComponent<R2, S, D>) this;
		else
			throw new ClassCastException("In component " + this + ", cannot cast " + getRole().getSimpleName() + " to " + type.getSimpleName());
	}
	
	@SuppressWarnings("unchecked")
	public default ScalarValue<?, S, D> cast(ScalarValue<?, ?, ?> value)
	{
		return (ScalarValue<?, S, D>) getDomain().cast(value);
	}

	@SuppressWarnings("unchecked")
	public default <S2 extends ValueDomainSubset<D2>, D2 extends ValueDomain> DataStructureComponent<R, S2, D2> as(S2 domain)
	{
		if (domain.isAssignableFrom(getDomain()))
			// safe
			return (DataStructureComponent<R, S2, D2>) this;
		else
			throw new ClassCastException("Cannot cast component " + this + " from " + getDomain() + " to " + domain);
	}
}
