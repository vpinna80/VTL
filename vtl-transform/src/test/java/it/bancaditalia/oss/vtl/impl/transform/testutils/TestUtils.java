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
package it.bancaditalia.oss.vtl.impl.transform.testutils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class TestUtils
{
	public static TransformationScheme mockSession(Map<VTLAlias, ? extends VTLValue> map) 
	{
		TransformationScheme session = mock(TransformationScheme.class, RETURNS_SMART_NULLS);
		
		// Mock getMetadata(alias)
		when(session.getMetadata(any(VTLAlias.class))).thenAnswer(p -> {
			VTLAlias name = p.getArgument(0);
			return Optional.ofNullable(map.get(name)).map(VTLValue::getMetadata).orElseThrow(() -> new VTLUnboundAliasException(name));
		});

		// Mock resolve(alias)
		when(session.resolve(any(VTLAlias.class))).thenAnswer(p -> {
			VTLAlias name = p.getArgument(0);
			return Optional.ofNullable(map.get(name)).orElseThrow(() -> new VTLUnboundAliasException(name));
		});
		
		when(session.getRepository()).then(p -> {
			MetadataRepository mock = mock(MetadataRepository.class, RETURNS_SMART_NULLS);
			when(mock.createTempVariable(any(VTLAlias.class), any())).then(i -> new TestComponent<>(i.getArgument(0), null, i.getArgument(1)).getVariable());
			return mock;
		});
		
		return session; 
	}

	public static DataSet concat(DataSet... samples)
	{
		return new AbstractDataSet(samples[0].getMetadata()) {
			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return Arrays.stream(samples).flatMap(DataSet::stream);
			}
		};
	}
}
