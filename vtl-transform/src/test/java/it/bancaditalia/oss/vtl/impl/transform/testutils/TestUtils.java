package it.bancaditalia.oss.vtl.impl.transform.testutils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import it.bancaditalia.oss.vtl.exceptions.VTLUnboundNameException;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class TestUtils {
	
	public static TransformationScheme mockSession(Map<String, DataSet> map) {
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
}
