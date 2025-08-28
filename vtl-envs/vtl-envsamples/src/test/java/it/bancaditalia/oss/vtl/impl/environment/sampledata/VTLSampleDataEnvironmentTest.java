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
package it.bancaditalia.oss.vtl.impl.environment.sampledata;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLValue;

public class VTLSampleDataEnvironmentTest
{
	public static Stream<Arguments> test()
	{
		return Arrays.stream(SampleDataSets.values()).map(Enum::toString).map(e -> Arguments.of(e));
	}
	
	@ParameterizedTest
	@MethodSource
	public void test(String name)
	{
		Optional<VTLValue> value = new VTLSampleDataEnvironment().getValue(null, VTLAliasImpl.of(name));
		assertTrue(value.isPresent());
		VTLValue dataset = value.get();
		assertTrue(dataset instanceof DataSet);
		
		((DataSet) dataset).forEach(dp -> {});
	}
}
