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
package it.bancaditalia.oss.vtl.impl.meta.json;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.engine.DMLStatement;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.domain.tcds.TransformationCriterionDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataStructureDefinition;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class JsonMetadataRepositoryTest
{
	@Test
	public void test() throws IOException
	{
		URL jsonURL = JsonMetadataRepositoryTest.class.getResource("test.json");

		DMLStatement statement = mock(DMLStatement.class);
		when(statement.getMetadata(any(TransformationScheme.class))).thenReturn(BOOLEAN);
		
		Engine engine = mock(Engine.class);
		when(engine.parseRules(anyString())).thenReturn(Stream.of(statement));
		
		JsonMetadataRepository repo = new JsonMetadataRepository(null, jsonURL, engine);
		
		Optional<ValueDomainSubset<?, ?>> domain1 = repo.getDomain(VTLAliasImpl.of("test_enum"));
		assertTrue(domain1.isPresent(), "Domain test_enum not found");
		assertInstanceOf(StringEnumeratedDomainSubset.class, domain1.get(), "Domain test_enum not enumerated");
		assertEquals(3, ((EnumeratedDomainSubset<?, ?, ?>) domain1.get()).getCodeItems().size(), "Domain test_enum codes != 3");

		Optional<ValueDomainSubset<?, ?>> domain2 = repo.getDomain(VTLAliasImpl.of("test_desc"));
		assertTrue(domain2.isPresent(), "Domain test_desc not found");
		assertInstanceOf(TransformationCriterionDomainSubset.class, domain2.get(), "Domain test_desc not described");

		Optional<Variable<?, ?>> var1 = repo.getVariable(VTLAliasImpl.of("VA1"));
		assertTrue(var1.isPresent(), "Variable Va1 not found");
		assertEquals(domain2.get(), var1.get().getDomain(), "Variable Va1 has incompatible domain");
		
		Optional<DataStructureDefinition> structureDefinition = repo.getStructureDefinition(VTLAliasImpl.of("str_1"));
		assertTrue(structureDefinition.isPresent(), "Structure str_1 not found");
		assertEquals(4, structureDefinition.get().size(), "Structure str_1 size != 4");
		
		Optional<VTLValueMetadata> ds1 = repo.getMetadata(VTLAliasImpl.of("ds_1"));
		assertTrue(ds1.isPresent(), "Dataset ds_1 not found");
		assertInstanceOf(DataSetStructure.class, ds1.get(), "ds1 is not a dataset");
		assertEquals(4, ((DataSetStructure) ds1.get()).size(), "ds1 components != 4");

		Optional<VTLValueMetadata> scalar1 = repo.getMetadata(VTLAliasImpl.of("start_date"));
		assertTrue(scalar1.isPresent(), "Scalar start_date not found");
		assertEquals(Domains.DATEDS, ((ScalarValueMetadata<?, ?>) scalar1.get()).getDomain(), "Variable Va1 has incompatible domain");
	}
}
