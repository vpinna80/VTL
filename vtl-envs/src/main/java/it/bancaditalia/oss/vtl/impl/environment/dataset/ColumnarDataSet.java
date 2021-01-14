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
package it.bancaditalia.oss.vtl.impl.environment.dataset;

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;

import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.Utils;

public class ColumnarDataSet extends AbstractDataSet
{
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarDataSet.class);
	
	private final Map<? extends DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>[]> columns;
	private final int nRows;

	public ColumnarDataSet(Map<? extends DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>[]> columns)
	{
		super(new DataStructureBuilder(columns.keySet()).build());
		this.columns = columns;
		nRows = columns.values().iterator().next().length;
		
		LOGGER.info("Indexing from source as {}", getMetadata());
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		return Utils.getStream(nRows)
				.mapToObj(this::mapIndexToDataPoint);
	}
	
	private DataPoint mapIndexToDataPoint(int rowIndex)
	{
		return Utils.getStream(columns.entrySet())
				.map(Utils.keepingKey(col -> col[rowIndex]))
				.collect(toDataPoint(getMetadata()));
	}
}
