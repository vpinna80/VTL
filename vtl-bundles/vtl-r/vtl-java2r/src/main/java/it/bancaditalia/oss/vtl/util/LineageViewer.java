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
package it.bancaditalia.oss.vtl.util;

import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.toList;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageSet;
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
	
	public Triple<String[], String[], Long[]> generateAdiacenceMatrix(TransformationScheme scheme)
	{
		Map<Entry<String, String>, Long> adiacences = new HashMap<>();
		Queue<Entry<Lineage, Long>> sources = new LinkedList<>();
		
		Map<Lineage, Long> result;
		try (Stream<DataPoint> stream = dataset.stream())
		{
			result = stream.map(DataPoint::getLineage)
					.collect(groupingByConcurrent(identity(), counting()));
		}
		sources.addAll(result.entrySet().stream().map(keepingValue(l -> l.resolveExternal(scheme))).collect(toList()));
		
		while (!sources.isEmpty())
		{
			Entry<Lineage, Long> entry = sources.poll();
			Lineage lineage = entry.getKey();
			// count of datapoints sharing the same lineage
			long count = entry.getValue();
			
			if (lineage instanceof LineageNode)
			{
				LineageSet sourceLineages = ((LineageNode) lineage).getSourceSet();
				Map<Lineage, Long> innerResult = ((LineageCall) sourceLineages).getSources().stream()
							.collect(toMapWithValues(k -> count));
				
				sources.addAll(innerResult.entrySet());
				for (Lineage source: innerResult.keySet())
					adiacences.merge(new SimpleEntry<>(source.toString(), lineage.toString()), innerResult.get(source), Long::sum);
			} 
		}
		
		Triple<String[], String[], Long[]> columns = new Triple<>(new String[adiacences.size()], new String[adiacences.size()], new Long[adiacences.size()]);
		int i = 0;
		for (Entry<Entry<String, String>, Long> link: adiacences.entrySet())
		{
			columns.getFirst()[i] = link.getKey().getKey();
			columns.getSecond()[i] = link.getKey().getValue();
			columns.getThird()[i] = link.getValue();
			
			i++;
		}
		
		return columns;
	}
}
