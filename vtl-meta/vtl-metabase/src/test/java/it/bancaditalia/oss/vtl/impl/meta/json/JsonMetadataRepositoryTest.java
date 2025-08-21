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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.domain.tcds.IntegerTransformationDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataStructureDefinition;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import jakarta.xml.bind.JAXBException;

public class JsonMetadataRepositoryTest
{
	@Test
	public void test() throws IOException, ClassNotFoundException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, JAXBException
	{
		URL jsonURL = JsonMetadataRepositoryTest.class.getResource("test.json");

		JsonMetadataRepository repo = new JsonMetadataRepository(null, jsonURL, new JavaVTLEngine());
		
		Optional<ValueDomainSubset<?, ?>> domain1 = repo.getDomain(VTLAliasImpl.of("test_enum_with_desc"));
		assertTrue(domain1.isPresent(), "Domain test_enum_with_desc not found");
		assertInstanceOf(StringEnumeratedDomainSubset.class, domain1.get(), "Domain test_enum_with_desc not enumerated");
		assertEquals(26, ((EnumeratedDomainSubset<?, ?, ?, ?>) domain1.get()).getCodeItems().size(), "Domain test_enum_with_desc codes != 26");
		@SuppressWarnings("unchecked")
		Optional<?> codeItemF = ((EnumeratedDomainSubset<?, ?, String, ?>) domain1.get()).getCodeItem("F");
		@SuppressWarnings("unchecked")
		Optional<?> codeItemE = ((EnumeratedDomainSubset<?, ?, String, ?>) domain1.get()).getCodeItem("E");
		assertTrue(codeItemE.isPresent(), "Domain test_enum_with_desc not contains 'E'");
		assertTrue(codeItemF.isPresent(), "Domain test_enum_with_desc not contains 'F'");
		assertEquals(StringValue.of("E"), codeItemE.get(), "Domain test_enum_with_desc 'E' != 'E'");
		assertEquals(StringValue.of("F"), codeItemF.get(), "Domain test_enum_with_desc 'F' != 'F'");

		Optional<ValueDomainSubset<?, ?>> domain2 = repo.getDomain(VTLAliasImpl.of("test_enum_vocals"));
		assertTrue(domain2.isPresent(), "Domain test_enum_letters not found");
		assertInstanceOf(StringEnumeratedDomainSubset.class, domain2.get(), "Domain test_enum_vocals not enumerated");
		assertEquals(5, ((EnumeratedDomainSubset<?, ?, ?, ?>) domain2.get()).getCodeItems().size(), "Domain test_enum_vocals codes != 5");
		@SuppressWarnings("unchecked")
		Optional<?> codeItemE2 = ((EnumeratedDomainSubset<?, ?, String, ?>) domain2.get()).getCodeItem("E");
		assertTrue(codeItemE2.isPresent(), "Domain test_enum_letters not contains 'E'");
		assertEquals(codeItemE.get(), codeItemE2.get(), "Domain test_enum_letters 'E' != 'E'");

		Optional<ValueDomainSubset<?, ?>> domain3 = repo.getDomain(VTLAliasImpl.of("test_desc_positive"));
		assertTrue(domain3.isPresent(), "Domain test_desc_positive not found");
		assertInstanceOf(IntegerTransformationDomainSubset.class, domain3.get(), "Domain test_desc_positive not described");
		assertTrue(((IntegerTransformationDomainSubset) domain3.get()).test(IntegerValue.of(10L)), "10 > 0");
		assertFalse(((IntegerTransformationDomainSubset) domain3.get()).test(IntegerValue.of(-10L)), "-10 < 0");

		Optional<ValueDomainSubset<?, ?>> domain4 = repo.getDomain(VTLAliasImpl.of("test_desc_perf_squares"));
		assertTrue(domain4.isPresent(), "Domain test_desc_perf_squares not found");
		assertInstanceOf(IntegerTransformationDomainSubset.class, domain4.get(), "Domain test_desc_perf_squares not described");
		assertTrue(((IntegerTransformationDomainSubset) domain4.get()).test(IntegerValue.of(16L)), "16 square");
		assertFalse(((IntegerTransformationDomainSubset) domain4.get()).test(IntegerValue.of(17L)), "17 not square");

		Optional<Variable<?, ?>> var1 = repo.getVariable(VTLAliasImpl.of("VA1"));
		assertTrue(var1.isPresent(), "Variable Va1 not found");
		assertEquals(domain3.get(), var1.get().getDomain(), "Variable Va1 has incompatible domain");
		
		Optional<DataStructureDefinition> structureDefinition = repo.getStructureDefinition(VTLAliasImpl.of("str_1"));
		assertTrue(structureDefinition.isPresent(), "Structure str_1 not found");
		assertEquals(4, structureDefinition.get().size(), "Structure str_1 size != 4");
		
		Optional<VTLValueMetadata> ds1 = repo.getMetadata(VTLAliasImpl.of("ds_1"));
		assertTrue(ds1.isPresent(), "Dataset ds_1 not found");
		assertInstanceOf(DataSetStructure.class, ds1.get(), "ds1 is not a dataset");
		assertEquals(4, ((DataSetStructure) ds1.get()).size(), "ds1 components != 4");

		Optional<VTLValueMetadata> scalar1 = repo.getMetadata(VTLAliasImpl.of("start_date"));
		assertTrue(scalar1.isPresent(), "Scalar start_date not found");
		assertEquals(DATEDS, ((ScalarValueMetadata<?, ?>) scalar1.get()).getDomain(), "Variable Va1 has incompatible domain");
	}
}
