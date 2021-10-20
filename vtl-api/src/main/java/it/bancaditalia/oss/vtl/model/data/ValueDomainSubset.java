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
package it.bancaditalia.oss.vtl.model.data;

/**
 * A subset of a {@link ValueDomain} as defined by VTL specification.
 * 
 * @author Valentino Pinna
 *
 * @param <D> The parent {@link ValueDomain}
 */
public interface ValueDomainSubset<S extends ValueDomainSubset<S, D>, D extends ValueDomain> extends ValueDomain
{
	/**
	 * @return the parent {@link ValueDomain}
	 */
	public D getParentDomain();
	
	/**
	 * Casts the given ScalarValue to this ValueDomainSubset 
	 * 
	 * @param value the {@link ScalarValue} to cast
	 * @return the casted {@link ScalarValue}
	 * @throws NullPointerException if the value is null
	 */
	public ScalarValue<?, ?, S, D> cast(ScalarValue<?, ?, ?, ?> value);
	
	/**
	 * If this domain has a default value defined, return it, otherwise return null.
	 * 
	 * @return the default value for this domain if defined. 
	 */
	public ScalarValue<?, ?, S, D> getDefaultValue();
	
	@Override
	boolean equals(Object obj);
	
	@Override
	int hashCode();
}
