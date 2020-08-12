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
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENGINE_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.SESSION_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class CSVFileEnvironmentTest
{
	private static final DataStructureComponent<?, ?, ?> IDENTIFIER = new DataStructureComponentImpl<>("IDENTIFIER", Identifier.class, DATEDS);
	private static final DataStructureComponent<?, ?, ?> MEASURE = new DataStructureComponentImpl<>("MEASURE", Measure.class, NUMBERDS);
	private static final DataStructureComponent<?, ?, ?> ATTRIBUTE = new DataStructureComponentImpl<>("ATTRIBUTE", Attribute.class, STRINGDS);

	private static CSVFileEnvironment INSTANCE;
	private static Path TEMPCSVFILE;
	private static String CSVALIAS;
	
	public static class Mock implements Engine, VTLSession
	{
		@Override
		public Stream<Statement> parseRules(String statements)
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public Stream<Statement> parseRules(Reader reader) throws IOException
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public Stream<Statement> parseRules(InputStream inputStream, Charset charset) throws IOException
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public Stream<Statement> parseRules(Path path, Charset charset) throws IOException
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public MetadataRepository getRepository()
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public VTLValue resolve(String node)
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public VTLValueMetadata getMetadata(String node)
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public Statement getRule(String node)
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public Engine getEngine()
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public Workspace getWorkspace()
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public VTLSession addStatements(String statements)
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public VTLSession addStatements(Reader reader) throws IOException
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public VTLSession addStatements(InputStream inputStream, Charset charset) throws IOException
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public VTLSession addStatements(Path path, Charset charset) throws IOException
		{
			throw new UnsupportedOperationException(); 
		}

		@Override
		public List<VTLValueMetadata> compile()
		{
			throw new UnsupportedOperationException(); 
		}
	}
	
	@BeforeAll
	public static void beforeClass() throws IOException
	{
		ENGINE_IMPLEMENTATION.setValue(Mock.class.getName());
		SESSION_IMPLEMENTATION.setValue(Mock.class.getName());
		
		INSTANCE = new CSVFileEnvironment();
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

	@Test
	public void containsTest()
	{
		assertTrue(INSTANCE.contains(CSVALIAS));
	}

	@Test
	public void getValueTest()
	{
		Optional<? extends VTLValue> search = INSTANCE.getValue(CSVALIAS);
		
		assertTrue(search.isPresent(), "Cannot find " + CSVALIAS);
		assertTrue(DataSet.class.isInstance(search.get()), "CSVALIAS is not a DataSet");
		
		DataSet dataset = (DataSet) search.get();
		
		assertEquals(3, dataset.getDataStructure().size(), "Wrong number of columns");
		assertTrue(dataset.getDataStructure().contains(IDENTIFIER), "Missing IDENTIFIER Column");
		assertTrue(dataset.getDataStructure().contains(MEASURE), "Missing MEASURE Column");
		assertTrue(dataset.getDataStructure().contains(ATTRIBUTE), "Missing ATTRIBUTE Column");
		
		assertEquals(7, dataset.stream().count(), "Wrong number of rows");
	}
}
