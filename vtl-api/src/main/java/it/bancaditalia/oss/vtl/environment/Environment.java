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
package it.bancaditalia.oss.vtl.environment;

import java.util.Optional;

import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

/**
 * A provider of VTL data objects. 
 * 
 * @author Valentino Pinna
 */
public interface Environment
{
	/**
	 * Returns an {@link Optional} reference to a VTL object with the specified name in this environment.
	 * 
	 * @param alias The name of requested object.
	 * @param repo A possibly null instance of a {@link MetadataRepository} used to fetch the metadata of the requested object
	 * @return An Optional possibly containing the metadata of the requested object if it was found in this environment.
	 */
	public default Optional<VTLValue> getValue(MetadataRepository repo, VTLAlias alias)
	{
		return Optional.empty();
	}
	
	/**
	 * Returns an {@link Optional} reference to the metadata of a VTL object with the specified name in this environment.
	 * NOTE: in most cases it is better to rely on the {@link MetadataRepository#getMetadata(VTLAlias)} method instead.
	 * 
	 * @param alias The name of requested object
	 * @return An Optional possibly containing the metadata of the requested object if it was found in this environment.
	 */
	public default Optional<VTLValueMetadata> getMetadata(VTLAlias alias)
	{
		return getValue(null, alias).map(DataSet.class::cast).map(DataSet::getMetadata);
	}

	/**
	 * Persistently store the given value in this environment for later use.
	 * 
	 * @param value The value to store
	 * @param alias The alias under which the value must be stored
	 * @return true if this environment was able to store the value.
	 */
	public default boolean store(VTLValue value, VTLAlias alias)
	{
		return false;
	}
}
