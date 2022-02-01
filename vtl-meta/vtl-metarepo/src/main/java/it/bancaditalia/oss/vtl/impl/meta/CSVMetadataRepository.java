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
package it.bancaditalia.oss.vtl.impl.meta;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.impl.meta.subsets.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;

public class CSVMetadataRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	
	public static final VTLProperty METADATA_CSV_SOURCE = 
			new VTLPropertyImpl("vtl.metadata.csv.source", "Path of a CSV file containing codelists", "C:/metadata.csv", true, false);
	
	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(CSVMetadataRepository.class, METADATA_CSV_SOURCE);
	}

	public CSVMetadataRepository() throws IOException
	{
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(METADATA_CSV_SOURCE.getValue()), UTF_8)))
		{
			reader.lines()
				.map(l -> l.split(",", 2))
				.collect(groupingByConcurrent(e -> e[0], mapping(e -> e[1], toSet())))
				.forEach((name, items) -> new StringCodeList(name, items));
		}
	}
}
