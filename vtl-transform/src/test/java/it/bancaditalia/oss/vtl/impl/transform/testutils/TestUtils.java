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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLUnboundNameException;
import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class TestUtils
{
	public static TransformationScheme mockSession(Map<String, ? extends VTLValue> map) 
	{
		TransformationScheme session = mock(TransformationScheme.class);
		
		// Mock getMetadata(alias)
		when(session.getMetadata(anyString())).thenAnswer(mock -> {
			String name = mock.getArgument(0);
			return Optional.ofNullable(map.get(name)).map(VTLValue::getMetadata).orElseThrow(() -> new VTLUnboundNameException(name));
		});

		// Mock resolve(alias)
		when(session.resolve(anyString())).thenAnswer(mock -> {
			String name = mock.getArgument(0);
			return Optional.ofNullable(map.get(name)).orElseThrow(() -> new VTLUnboundNameException(name));
		});
		
		return session; 
	}

	public static DataSet concat(DataSet... samples)
	{
		return new AbstractDataSet(samples[0].getMetadata()) {
			private static final long serialVersionUID = 1L;
	
			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return Arrays.stream(samples)
						.map(DataSet::stream)
						.reduce(Stream::concat)
						.get();
			}
		};
	}
}
