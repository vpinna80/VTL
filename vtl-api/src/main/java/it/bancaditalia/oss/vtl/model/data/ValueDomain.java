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

import java.io.Serializable;

/**
 * A standard unrestricted VTL valuedomain as defined in VTL specification. 
 * 
 * @author Valentino Pinna
 */
public interface ValueDomain extends Serializable
{
	/**
	 * Check if a value of a given ValueDomain can be converted to a value of this ValueDomain
	 *  
	 * @param other the other {@link ValueDomain}
	 * @return true if the conversion is possible
	 */
	public boolean isAssignableFrom(ValueDomain other);

	/**
	 * Check if a value of a given ValueDomain can be compared to a value of this ValueDomain
	 *  
	 * @param other the other {@link ValueDomain}
	 * @return true if the comparison is possible
	 */
	public boolean isComparableWith(ValueDomain other);

	/**
	 * @return the default variable name for {@link DataStructureComponent}s of this ValueDomain
	 */
	public String getVarName();
}
