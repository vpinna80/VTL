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
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.answer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import java.io.FileNotFoundException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import it.bancaditalia.oss.sdmx.api.BaseObservation;
import it.bancaditalia.oss.sdmx.api.Codelist;
import it.bancaditalia.oss.sdmx.api.DataFlowStructure;
import it.bancaditalia.oss.sdmx.api.DoubleObservation;
import it.bancaditalia.oss.sdmx.api.PortableTimeSeries;
import it.bancaditalia.oss.sdmx.client.SdmxClientHandler;
import it.bancaditalia.oss.sdmx.exceptions.SdmxException;
import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerCollectors;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
public class SDMXEnvironmentTest
{
	private static final String PROVIDER = "ECB";
	private static final String DATAFLOW = "EXR";
	private static final String QUERY = DATAFLOW + "/" + "A.USD..SP00.A";
	private static final String SDMX_ALIAS = PROVIDER + ":" + QUERY;
	private static final List<PortableTimeSeries<Double>> SAMPLE;
	private static final DataFlowStructure DSD;
	
	static {
		Kryo kryo = new Kryo();
		kryo.register(DoubleObservation.class, new Serializer<DoubleObservation>() {

			@Override
			public void write(Kryo k, Output output, DoubleObservation object)
			{
				throw new UnsupportedOperationException();
			}

			@Override
			public DoubleObservation read(Kryo k, Input input, Class<DoubleObservation> type)
			{
				k.readClass(input);
				String timeslot = (String) k.readClassAndObject(input);
				Double value = (Double) k.readClassAndObject(input);
				@SuppressWarnings("unchecked")
				Map<String, String> attrs = (Map<String, String>) k.readClassAndObject(input);
				return new DoubleObservation(timeslot, value, attrs);
			}
		});
		kryo.register(PortableTimeSeries.class, new Serializer<PortableTimeSeries<?>>() {

			@Override
			public void write(Kryo k, Output output, PortableTimeSeries<?> object)
			{
				throw new UnsupportedOperationException();
			}

			@SuppressWarnings("unchecked")
			@Override
			public PortableTimeSeries<?> read(Kryo k, Input input, Class<PortableTimeSeries<?>> type)
			{
				k.readClass(input);
				Map<String, Entry<String, String>> dims = ((Map<String, String>) k.readClassAndObject(input)).entrySet().stream()
						.map(keepingKey(v -> new SimpleEntry<>(v, v)))
						.collect(SerCollectors.entriesToMap());
				Map<String, String> attrs = (Map<String, String>) k.readClassAndObject(input);
				int size = (int) k.readClassAndObject(input);
				PortableTimeSeries<Object> pts = new PortableTimeSeries<>();
				pts.setDimensions(dims);
				pts.setAttributes(attrs);
				for (int i = 0; i < size; i++)
					pts.add((BaseObservation<Object>) k.readClassAndObject(input));
				return pts;
			}
		});

		try (Input kryoIn = new Input(SDMXEnvironmentTest.class.getResourceAsStream("serialized-dsd.bin")))
		{
			DSD = kryo.readObject(kryoIn, DataFlowStructure.class);
			
			// Bug in connectors workaround
			Stream.concat(DSD.getDimensions().stream(), DSD.getAttributes().stream())
				.forEach(component -> {
					Codelist codeList = component.getCodeList();
					if (codeList != null && codeList.getId() == null)
						codeList.setId("CL_" + component.getId());
				});
		} 

		try (Input kryoIn = new Input(SDMXEnvironmentTest.class.getResourceAsStream("serialized-series.bin")))
		{
			@SuppressWarnings("unchecked")
			List<PortableTimeSeries<Double>> temp = (List<PortableTimeSeries<Double>>) kryo.readClassAndObject(kryoIn);
			SAMPLE = temp;
		}
	}
	
	@BeforeAll
	public static void beforeAll()
	{
		// Mock configuration manager
		ConfigurationManager mockConfman = mock(ConfigurationManager.class);
		MockedStatic<ConfigurationManager> staticConfmanMock = mockStatic(ConfigurationManager.class);
		staticConfmanMock.when(ConfigurationManager::getDefault).thenReturn(mockConfman);
		CONFIG_MANAGER.setValue(mockConfman.getClass().getName());
		
		// Mock repository
		HashMap<Object, Object> domains = new HashMap<>();
		MetadataRepository mockRepo = mock(MetadataRepository.class);
		when(mockConfman.getMetadataRepository()).thenReturn(mockRepo);
		when(mockRepo.defineDomain(anyString(), any(StringEnumeratedDomainSubset.class)))
			.then(answer((String id, Class<? extends ValueDomain> cls, Set<String> set) -> {
				if (domains.containsKey(id))
					return domains.get(id);
				StringEnumeratedDomainSubset<?, ?, ?, ?> domainMock = mock(StringEnumeratedDomainSubset.class, id + ":string");
				domains.put(id, domainMock);
				when(domainMock.getName()).thenReturn(id);
				when(domainMock.cast(any())).then(answer(v -> v));
				return domainMock;
			}));
		when(mockRepo.getDomain(anyString())).then(answer(domains::get));
		
		// Mock SDMX connectors 
		MockedStatic<SdmxClientHandler> handlerMock = mockStatic(SdmxClientHandler.class);
		handlerMock.when(() -> SdmxClientHandler.getDataFlowStructure(PROVIDER, DATAFLOW)).thenReturn(DSD);
		handlerMock.when(() -> SdmxClientHandler.getTimeSeries(PROVIDER, QUERY, null, null)).thenReturn(SAMPLE);
		handlerMock.when(SdmxClientHandler::getProviders).thenReturn(new TreeMap<>(singletonMap("ECB", false)));
		handlerMock.when(() -> SdmxClientHandler.getCodes(anyString(), anyString(), anyString()))
			.thenAnswer(answer((String provider, String dataflow, String id) -> DSD.getDimension(id).getCodeList()));
	}

	@Test
	public void containsTest()
	{
		assertTrue(new SDMXEnvironment().contains(SDMX_ALIAS));
	}
	
	@Test
	public void getValueTest()
	{
		Optional<? extends VTLValue> search = new SDMXEnvironment().getValue(SDMX_ALIAS);

		assertTrue(search.isPresent(), "Cannot find " + SDMX_ALIAS);
		assertTrue(DataSet.class.isInstance(search.get()), "CSVALIAS is not a DataSet");
		
		DataSet dataset = (DataSet) search.get();
		
		assertEquals(31, dataset.getMetadata().size(), "Wrong number of columns");
		assertEquals(21, dataset.stream().count(), "Wrong number of rows");
	}
	
	@Test
	public void getValueMetadataTest() throws SdmxException, KryoException, FileNotFoundException
	{
		Optional<VTLValueMetadata> search = new SDMXEnvironment().getValueMetadata(SDMX_ALIAS);

		assertTrue(search.isPresent(), "Cannot find " + SDMX_ALIAS);
		assertTrue(DataSetMetadata.class.isInstance(search.get()), SDMX_ALIAS + " is not a DataSet");
		
		DataSetMetadata dataset = (DataSetMetadata) search.get();
		assertEquals(31, dataset.size(), "Wrong number of columns");
	}
}
