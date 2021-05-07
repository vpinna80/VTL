package it.bancaditalia.oss.vtl.util;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingByConcurrent;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.types.lineage.LineageChain;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class LineageViewer
{
	private final DataSet dataset;

	public LineageViewer(DataSet dataset)
	{
		this.dataset = dataset;
	}
	
	public Object[][] generateAdiacenceMatrix(TransformationScheme scheme)
	{
		Map<Lineage, Map<Lineage, Long>> adiacences = new HashMap<>();
		Queue<Map<Lineage, Long>> sources = new LinkedList<>();
		
		try (Stream<DataPoint> stream = dataset.stream())
		{
			Map<Lineage, Long> result = stream.map(DataPoint::getLineage)
					.map(l -> l.resolveExternal(scheme))
					.collect(groupingByConcurrent(identity(), counting()));
			sources.add(result);
		}
		
		while (!sources.isEmpty())
			for (Entry<Lineage, Long> entry: sources.poll().entrySet())
			{
				Lineage lineage = entry.getKey();
				long count = entry.getValue();
				if (lineage instanceof LineageNode)
				{
					Map<Lineage, Long> innerResult = ((LineageNode) lineage).getSources().stream().collect(groupingByConcurrent(identity(), counting()));
					innerResult.replaceAll((k, v) -> v * count);
					sources.add(innerResult);
					for (Lineage source: innerResult.keySet())
						adiacences.computeIfAbsent(source, s -> new HashMap<>()).merge(lineage, innerResult.get(source), Long::sum);
				} 
				else if (lineage instanceof LineageChain)
					throw new UnsupportedOperationException();
			}
		
		Object[][] toStringValue = new Object[3][adiacences.size()];
		toStringValue[0] = new Object[adiacences.size()];
		toStringValue[1] = new Object[adiacences.size()];
		toStringValue[2] = new Object[adiacences.size()];
		int i = 0;
		for (Lineage source: adiacences.keySet())
		{
			for (Entry<Lineage, Long> target: adiacences.get(source).entrySet())
			{
				toStringValue[0][i] = source.toString();
				toStringValue[1][i] = target.getKey().toString();
				toStringValue[2][i] = target.getValue();
			}
			i++;
		}
		
		return toStringValue;
	}
}
