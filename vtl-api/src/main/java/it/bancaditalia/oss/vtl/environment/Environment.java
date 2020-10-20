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
package it.bancaditalia.oss.vtl.environment;

import java.util.Optional;

import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;

/**
 * A provider of VTL data objects. 
 * 
 * @author Valentino Pinna
 */
public interface Environment
{
	/**
	 * Checks if this environment provides a VTL object with the specified name.
	 * 
	 * @param name The name of requested object.
	 * @return true if this environment provides the specified object.
	 */
	public boolean contains(String name);
	
	/**
	 * Returns an {@link Optional} reference to a VTL object with the specified name in this environment.
	 * 
	 * @param name The name of requested object.
	 * @return An Optional with a reference to the requested object o {@link Optional#empty()} if the object is not found in this environment.
	 */
	public Optional<VTLValue> getValue(String name);

	/**
	 * Returns an {@link Optional} reference to the metadata of a VTL object with the specified name in this environment.
	 * 
	 * @param name The name of requested object
	 * @return An Optional with a reference to the metadata of the requested object o {@link Optional#empty()} if the object is not found in this environment.
	 */
	public default Optional<VTLValueMetadata> getValueMetadata(String name)
	{
		return getValue(name).map(VTLValue::getMetadata);
	}
	
	/**
	 * Implementing classes may override this method if they need to use a particular initialization procedure.
	 * 
	 * @param configuration Parameters that may be passed to the implementing class.
	 * 
	 * @return {@code this} instance, initialized.
	 */
	public default Environment init(Object... configuration)
	{
		return this;
	}
}
