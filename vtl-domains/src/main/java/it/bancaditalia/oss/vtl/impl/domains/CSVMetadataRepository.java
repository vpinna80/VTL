/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.domains;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import it.bancaditalia.oss.vtl.model.domain.StringCodeList;

public class CSVMetadataRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	
	public static final String METADATA_CSV_SOURCE = "vtl.metadata.csv.source";

	public CSVMetadataRepository() throws IOException
	{
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(System.getProperty(METADATA_CSV_SOURCE)), UTF_8)))
		{
			reader.lines()
				.map(l -> new SimpleEntry<>(l.split(",", 2)[0], l.split(",", 2)[1]))
				.collect(groupingByConcurrent(Entry::getKey, mapping(Entry::getValue, toSet())))
				.forEach(defineDomainOf(StringCodeList.class));
		}
	}
}
