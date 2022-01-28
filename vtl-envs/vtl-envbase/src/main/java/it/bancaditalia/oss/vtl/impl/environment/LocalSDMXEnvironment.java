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
package it.bancaditalia.oss.vtl.impl.environment;

import java.io.File;
import java.util.List;
import java.util.Optional;

import it.bancaditalia.oss.sdmx.api.PortableTimeSeries;
import it.bancaditalia.oss.sdmx.client.SdmxClientHandler;
import it.bancaditalia.oss.sdmx.exceptions.DataStructureException;
import it.bancaditalia.oss.sdmx.exceptions.SdmxException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.environment.exceptions.VTLInputException;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;

class LocalSDMXEnvironment extends SDMXEnvironment
{
	private static final long serialVersionUID = 1L;
	private final String dirPath;
	private final String provider;
	
	public LocalSDMXEnvironment(String provider, String dirPath) throws SdmxException
	{
		this.dirPath = dirPath;
		this.provider = provider;
		SdmxClientHandler.addLocalProvider(provider, dirPath, "VTL File SDMX Environment - " + provider);
	}

	@Override
	public boolean contains(String id)
	{
		String tokens[] = id.split("[.]", 2);
		String flow = tokens[0];
		String tsid = tokens[1];
		return tokens.length == 2 && new File(dirPath + File.separator + "data_" + flow + "_" + tsid + ".xml").exists();
	}

	@Override
	public Optional<VTLValue> getValue(String name)
	{
		if (contains(name))
			try
			{
				List<PortableTimeSeries<Double>> table = SdmxClientHandler.getTimeSeries(provider, name, null, null);
				return Optional.of(parseSDMXTable(name, table));
			}
			catch (SdmxException | DataStructureException e)
			{
				throw new VTLNestedException("Fatal error contacting SDMX provider \"" + provider + "\"", e);
			}

		return Optional.empty();
	}	
	
	@Override
	public Optional<VTLValueMetadata> getValueMetadata(String name)
	{
		String tokens[] = name.split("\\.", -1); //avoid discard trailing blanks
		if (tokens.length >= 2)
		{
			String dataflow = tokens[0];
			if (contains(name)) {
				return Optional.ofNullable(getMetadataSDMX(provider, dataflow, tokens));
			}
		}
		else {
			//we should not get here anyway
			throw new VTLInputException("Error in variable name for SDMX environment.");
		}

		return Optional.empty();
	}
}
