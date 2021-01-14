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
public interface ValueDomainSubset<D extends ValueDomain> extends ValueDomain
{
	/**
	 * @return A criterion, if defined, to limit the admissible values from the parent {@link ValueDomain}
	 */
	public Object getCriterion();

	/**
	 * @return the parent {@link ValueDomain}
	 */
	public D getParentDomain();
	
	/**
	 * Casts the given ScalarValue to this ValueDomainSubset 
	 * 
	 * @param value the {@link ScalarValue} to cast
	 * @return the casted {@link ScalarValue}
	 */
	public ScalarValue<?, ? extends ValueDomainSubset<? extends D>, ? extends D> cast(ScalarValue<?, ?, ?> value);
}
