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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.CONFIG_MANAGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

@ExtendWith(MockitoExtension.class)
public class CSVFileEnvironmentTest
{
	private static final MockedStatic<ConfigurationManager> CONFMAN_MOCK = mockStatic(ConfigurationManager.class);
	private static final DataStructureComponent<?, ?, ?> IDENTIFIER = DataStructureComponentImpl.of("IDENTIFIER", Identifier.class, DATEDS);
	private static final DataStructureComponent<?, ?, ?> MEASURE = DataStructureComponentImpl.of("MEASURE", Measure.class, NUMBERDS);
	private static final DataStructureComponent<?, ?, ?> ATTRIBUTE = DataStructureComponentImpl.of("ATTRIBUTE", Attribute.class, STRINGDS);
	private static final DataStructureComponent<?, ?, ?> QUOTED = DataStructureComponentImpl.of("QUOTED", Attribute.class, STRINGDS);
	private static final String QUOTED_RESULTS[] = {
			" Hello, \"World\"! ",
			"Test with",
			"",
			"Dummy",
			"Dummy",
			" Dummy",
			"Dummy"
		};
	
	
	private static Path TEMPCSVFILE;
	private static String CSVALIAS;

	@BeforeAll
	public static void beforeAll() throws IOException
	{
		MetadataRepository metarepo = mock(MetadataRepository.class, RETURNS_SMART_NULLS);
		when(metarepo.getStructure(anyString())).thenReturn(null);
		ConfigurationManager mockConfman = mock(ConfigurationManager.class, RETURNS_SMART_NULLS);
		CONFMAN_MOCK.when(ConfigurationManager::getDefault).thenReturn(mockConfman);
		when(mockConfman.getMetadataRepository()).thenReturn(metarepo);
		CONFIG_MANAGER.setValue(mockConfman.getClass().getName());
		
		TEMPCSVFILE = Files.createTempFile(null, ".csv").toAbsolutePath();
		InputStream csv = CSVFileEnvironment.class.getResourceAsStream("test.csv");
		assertNotNull(csv, "CSV test file not found");
		FileOutputStream fos = new FileOutputStream(TEMPCSVFILE.toString());
		int c;
		while ((c = csv.read()) >= 0)
			fos.write(c);
		
		fos.close();
		csv.close();
		CSVALIAS = "csv:" + TEMPCSVFILE;
	}
	
	@AfterAll
	public static void afterClass() throws InterruptedException
	{
		CONFMAN_MOCK.close();
		
		try
		{
			Files.deleteIfExists(TEMPCSVFILE);
		}
		catch (IOException e)
		{
			// ignore
		}
	}	

	@Test
	public void containsTest()
	{
		assertTrue(new CSVFileEnvironment().contains(CSVALIAS));
	}

	@Test
	public void getValueTest()
	{
		Optional<? extends VTLValue> search = new CSVFileEnvironment().getValue(CSVALIAS);
		
		assertTrue(search.isPresent(), "Cannot find " + CSVALIAS);
		assertTrue(DataSet.class.isInstance(search.get()), CSVALIAS + " is not a DataSet");
		
		DataSet dataset = (DataSet) search.get();
		
		final DataSetMetadata structure = dataset.getMetadata();
		assertEquals(4, structure.size(), "Wrong number of columns");
		assertTrue(structure.contains(IDENTIFIER), "Missing IDENTIFIER Column");
		assertTrue(structure.contains(MEASURE), "Missing MEASURE Column");
		assertTrue(structure.contains(ATTRIBUTE), "Missing ATTRIBUTE Column");
		assertTrue(structure.contains(QUOTED), "Missing QUOTED Column");
		
		Set<String> results = new HashSet<>(Arrays.asList(QUOTED_RESULTS));
		try (Stream<DataPoint> stream = dataset.stream())
		{
			long count = stream.map(dp -> dp.get(QUOTED))
				.map(ScalarValue::get)
				.map(c -> c == null ? "" : c.toString())
				.peek(s -> assertTrue(results.contains(s), "Result '" + s + "' not found."))
				.count();
			assertEquals(7, count, "Wrong number of rows");
		}
	}
}
