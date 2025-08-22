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
import static it.bancaditalia.oss.vtl.impl.data.samples.SampleDataSets.SAMPLE11;
import static it.bancaditalia.oss.vtl.impl.data.samples.SampleDataSets.SAMPLE12;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.exceptions.base.MockitoException;

import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class DBMSEnvironmentTest
{
	@Test
	public void test() throws SQLException, MalformedURLException
	{
		DBMSEnvironment env = new DBMSEnvironment("jdbc:hsqldb:mem:schema;shutdown=true", "sa", "", "schema", "schema", "");
		
		env.loadDatasets(Map.of("sample10", SAMPLE10, "sample11", SAMPLE11, "sample12", SAMPLE12));
		
		MetadataRepository repo = mock(MetadataRepository.class);
		when(repo.getMetadata(any(VTLAlias.class))).thenAnswer(args -> {
			switch (args.getArgument(0).toString().toLowerCase())
			{
				case "sample10": return Optional.of(SAMPLE10.getMetadata());
				case "sample11": return Optional.of(SAMPLE11.getMetadata());
				case "sample12": return Optional.of(SAMPLE12.getMetadata());
				default: throw new MockitoException("Unexpected value");
			}
		});
		
		Optional<VTLValue> sample10 = env.getValue(repo, SAMPLE10.getAlias());
		assertTrue(sample10.isPresent(), "sample10 exists");
		assertInstanceOf(DataSet.class, sample10.get(), "sample10 is dataset");
		assertEquals(SAMPLE10.getMetadata(), ((DataSet) sample10.get()).getMetadata(), "sample10 metadata");
		List<DataPoint> expected10 = SAMPLE10.stream().collect(toList());
		List<DataPoint> actual10 = ((DataSet) sample10.get()).stream().collect(toList());
		assertEquals(expected10.size(), actual10.size(), "DP Count differs");
		for (DataPoint dp: actual10)
			assertTrue(expected10.contains(dp), "Unexpected DataPoint: " + dp + " in \n" + expected10);
		for (DataPoint dp: expected10)
			assertTrue(actual10.contains(dp), "Missing DataPoint: " + dp + " in \n" + actual10);

		Optional<VTLValue> sample11 = env.getValue(repo, SAMPLE11.getAlias());
		assertTrue(sample11.isPresent(), "sample11 exists");
		assertInstanceOf(DataSet.class, sample11.get(), "sample11 is dataset");
		assertEquals(SAMPLE11.getMetadata(), ((DataSet) sample11.get()).getMetadata(), "sample11 metadata");
		List<DataPoint> expected11 = SAMPLE11.stream().collect(toList());
		List<DataPoint> actual11 = ((DataSet) sample11.get()).stream().collect(toList());
		assertEquals(expected11.size(), actual11.size(), "DP Count differs");
		for (DataPoint dp: actual11)
			assertTrue(expected11.contains(dp), "Unexpected DataPoint: " + dp + " in \n" + expected11);
		for (DataPoint dp: expected11)
			assertTrue(actual11.contains(dp), "Missing DataPoint: " + dp + " in \n" + actual11);

		Optional<VTLValue> sample12 = env.getValue(repo, SAMPLE12.getAlias());
		assertTrue(sample12.isPresent(), "sample12 exists");
		assertInstanceOf(DataSet.class, sample12.get(), "sample12 is dataset");
		assertEquals(SAMPLE12.getMetadata(), ((DataSet) sample12.get()).getMetadata(), "sample12 metadata");
		List<DataPoint> expected12 = SAMPLE12.stream().collect(toList());
		List<DataPoint> actual12 = ((DataSet) sample12.get()).stream().collect(toList());
		assertEquals(expected12.size(), actual12.size(), "DP Count differs");
		for (DataPoint dp: actual12)
			assertTrue(expected12.contains(dp), "Unexpected DataPoint: " + dp + " in \n" + expected12);
		for (DataPoint dp: expected12)
			assertTrue(actual12.contains(dp), "Missing DataPoint: " + dp + " in \n" + actual12);
	}
}
