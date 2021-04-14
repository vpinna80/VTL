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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.sdmx.util.Utils.Function;
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;

public class CSVPathEnvironment extends CSVFileEnvironment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(CSVPathEnvironment.class);
	public static final VTLProperty VTL_CSV_ENVIRONMENT_SEARCH_PATH = 
			new VTLPropertyImpl("vtl.csv.search.path", "Path to search for CSV files", System.getenv("PATH"), true, false, System.getenv("PATH"));

	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(CSVPathEnvironment.class, VTL_CSV_ENVIRONMENT_SEARCH_PATH);
	}
	
	private List<String> paths;
	
	public CSVPathEnvironment()
	{
		paths = Arrays.asList(VTL_CSV_ENVIRONMENT_SEARCH_PATH.getValue().split(File.pathSeparator));
	}
	
	@Override
	public boolean contains(String name)
	{
		if (name.startsWith("csv:"))
			if (name.contains("\\") || name.contains("/"))
				return super.contains(name);
			else
				return searchPaths(name.substring(4)).isPresent();
		else
			return false;
	}

	private Optional<Path> searchPaths(String fileName)
	{
		return paths.stream()
			.map(path -> Paths.get(path, fileName))
			.filter(Files::exists)
			.findAny();
	}

	@Override
	public Optional<VTLValue> getValue(String alias)
	{
		return mapper(alias, super::getValue);
	}

	private <T> Optional<T> mapper(String alias, Function<String, Optional<T>> mapper)
	{
		return searchPaths(alias.substring(4))
				.map(Path::toString)
				.map(path -> "csv:" + path)
				.map(path -> { LOGGER.info("Found {} in {}", alias, path); return path; })
				.map(string -> mapper.apply(string))
				.orElse(Optional.empty());
	}
	
	@Override
	public Optional<VTLValueMetadata> getValueMetadata(String alias)
	{
		return mapper(alias, super::getValueMetadata);
	}
}
