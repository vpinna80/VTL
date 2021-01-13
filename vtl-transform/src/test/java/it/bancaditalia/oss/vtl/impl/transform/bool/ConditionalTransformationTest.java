package it.bancaditalia.oss.vtl.impl.transform.bool;

import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE3;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE5;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE6;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class ConditionalTransformationTest
{
	// cond:  true, true, null, false, false, true
	// left:   11L,  null,  13L,   14L,   15L,  16L
	// right:  21L,   22L,  23L,   24L,   25L,  26L
	private static final Long[] INTEGER_RESULTS = { 11L, null, 23L, 24L, 25L, 16L };

	private TransformationScheme session;
	private VarIDOperand cond, left, right;
	
	@BeforeEach
	public void before()
	{
		cond = new VarIDOperand("cond");
		left = new VarIDOperand("left");
		right = new VarIDOperand("right");
		Map<String, DataSet> map = new HashMap<>();
		map.put("cond", SAMPLE3);
		map.put("left", SAMPLE5);
		map.put("right", SAMPLE6);
		session = TestUtils.mockSession(map);
	}
	
	@Test
	public void test()
	{
		ConditionalTransformation arTransformation = new ConditionalTransformation(cond, left, right);
		
		DataSetMetadata metadata = (DataSetMetadata) arTransformation.getMetadata(session);
		assertTrue(metadata.contains("INTEGER_1"));
		
		DataSet computedResult = (DataSet) arTransformation.eval(session);
		
		assertEquals(INTEGER_RESULTS.length, computedResult.size());
		assertEquals(metadata, computedResult.getMetadata());
		
		DataStructureComponent<?, ?, ?> id = metadata.getComponent("STRING_1").get();
		DataStructureComponent<?, ?, ?> measure = metadata.getComponent("INTEGER_1").get();
		
		computedResult.stream()
			.map(dp -> new SimpleEntry<>(dp.get(id).get().toString().charAt(0) - 'A', dp.get(measure).get()))
			.forEach(e -> assertEquals(INTEGER_RESULTS[e.getKey()], e.getValue(), "" + (char)(e.getKey() + 'A')));
	}
}
