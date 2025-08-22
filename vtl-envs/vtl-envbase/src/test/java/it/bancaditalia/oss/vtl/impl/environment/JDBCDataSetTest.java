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

import static it.bancaditalia.oss.vtl.impl.data.samples.SampleDataSets.SAMPLE10;
import static it.bancaditalia.oss.vtl.impl.data.samples.SampleDataSets.SAMPLE5;
import static it.bancaditalia.oss.vtl.impl.data.samples.SampleDataSets.SAMPLE6;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.exceptions.base.MockitoException;

import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class JDBCDataSetTest
{
	private static DataSet sample5;
	private static DataSet sample6;
	private static DataSet sample10;

	@BeforeAll
	public static void init() throws SQLException
	{
		DBMSEnvironment env = new DBMSEnvironment("jdbc:hsqldb:mem:schema;shutdown=true", "sa", "", "schema", "schema", "");
		env.loadDatasets(Map.of("sample5", SAMPLE5, "sample6", SAMPLE6, "sample10", SAMPLE10));
	
		MetadataRepository repo = mock(MetadataRepository.class);
		when(repo.getMetadata(any(VTLAlias.class))).thenAnswer(args -> {
			switch (args.getArgument(0).toString().toLowerCase())
			{
				case "sample5": return Optional.of(SAMPLE5.getMetadata());
				case "sample6": return Optional.of(SAMPLE6.getMetadata());
				case "sample10": return Optional.of(SAMPLE10.getMetadata());
				default: throw new MockitoException("Unexpected value");
			}
		});

		sample5 = (DataSet) env.getValue(repo, SAMPLE5.getAlias()).get();
		sample6 = (DataSet) env.getValue(repo, SAMPLE6.getAlias()).get();
		sample10 = (DataSet) env.getValue(repo, SAMPLE10.getAlias()).get();
	}
	
	@Test
	public void testMembership()
	{
		VTLAlias string_1 = VTLAliasImpl.of("string_1");
		DataSet membership = sample10.membership(string_1, identity());
		assertEquals(sample10.size(), membership.size(), "Size differs");
		assertEquals(sample10.getMetadata().membership(string_1), membership.getMetadata(), "Structure differs");
	}
	
	@Test
	public void testSubspace()
	{
		DataSetComponent<Identifier, ?, ?> subspaceId = sample10.getComponent(VTLAliasImpl.of("string_1")).get().asRole(Identifier.class);
		DataSet subspace = sample10.subspace(Map.of(subspaceId, StringValue.of("A")), identity());
		assertEquals(1, subspace.size(), "Size differs");
		assertEquals(sample10.getMetadata().subspace(Set.of(subspaceId)), subspace.getMetadata(), "Structure differs");
	}

	@Test
	public void testUnion()
	{
		DataSet union = sample5.union(List.of(sample6, sample10), identity());
		assertEquals(9, union.size(), "Size differs");
		assertEquals(sample10.getMetadata(), union.getMetadata(), "Structure differs");
	}
}
