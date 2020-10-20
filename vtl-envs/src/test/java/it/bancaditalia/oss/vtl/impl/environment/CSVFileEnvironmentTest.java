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
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.CONFIG_MANAGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLValue;

@ExtendWith(MockitoExtension.class)
public class CSVFileEnvironmentTest
{
	private static final DataStructureComponent<?, ?, ?> IDENTIFIER = new DataStructureComponentImpl<>("IDENTIFIER", Identifier.class, DATEDS);
	private static final DataStructureComponent<?, ?, ?> MEASURE = new DataStructureComponentImpl<>("MEASURE", Measure.class, NUMBERDS);
	private static final DataStructureComponent<?, ?, ?> ATTRIBUTE = new DataStructureComponentImpl<>("ATTRIBUTE", Attribute.class, STRINGDS);

	private static Path TEMPCSVFILE;
	private static String CSVALIAS;
	
	@Mock
	ConfigurationManager confman;

	@BeforeAll
	public static void beforeClass() throws IOException
	{
		TEMPCSVFILE = Files.createTempFile(null, ".csv").toAbsolutePath();
		InputStream csv = CSVFileEnvironmentTest.class.getResourceAsStream("test.csv");
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
		try
		{
			Files.deleteIfExists(TEMPCSVFILE);
		}
		catch (IOException e)
		{
			// ignore
		}
	}	
	
	@BeforeEach
	public void beforeEach()
	{
		CONFIG_MANAGER.setValue(confman.getClass().getName());
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
		
		assertEquals(3, dataset.getMetadata().size(), "Wrong number of columns");
		assertTrue(dataset.getMetadata().contains(IDENTIFIER), "Missing IDENTIFIER Column");
		assertTrue(dataset.getMetadata().contains(MEASURE), "Missing MEASURE Column");
		assertTrue(dataset.getMetadata().contains(ATTRIBUTE), "Missing ATTRIBUTE Column");
		
		try (Stream<DataPoint> stream = dataset.stream())
		{
			assertEquals(7, stream.count(), "Wrong number of rows");
		}
	}
}
