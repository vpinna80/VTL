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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toConcurrentMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.data.date.DateHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;

public class Paginator implements AutoCloseable
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Paginator.class);
	
	private final BlockingQueue<DataPoint> queue = new ArrayBlockingQueue<>(100);
	private final DataSetMetadata dataStructure;

	private boolean closed = false;
	private RuntimeException lastException = null;
	private Map<String, String> toBeCast = new HashMap<String, String>(); 
	
	public Paginator(DataSet dataset)
	{
		dataStructure = dataset.getMetadata();
		Thread thread = new Thread(() -> {
			try (Stream<DataPoint> stream = dataset.stream())
			{
				stream.forEach(dp -> {
					while (!isClosed())
						try
						{
							queue.put(dp);
							break;
						}
						catch (InterruptedException e)
						{
							close();
							Thread.currentThread().interrupt();
						}
				});
			}
			catch (Exception e)
			{
				LOGGER.error(e.getMessage(), e);
			}
			finally
			{
				close();
			}
		});
		thread.setDaemon(true);
		thread.start();
	}

	public boolean isClosed()
	{
		return closed;
	}

	@Override
	public void close()
	{
		closed = true;
	}

	public DataSetMetadata getDataStructure()
	{
		return dataStructure;
	}

	public List<DataPoint> moreDataPoints()
	{
		return moreDataPoints(20);
	}

	public List<DataPoint> moreDataPoints(int size)
	{
		List<DataPoint> result = new ArrayList<>();

		while ((!isClosed() || !queue.isEmpty()) && (size <= 0 || result.size() < size))
			try
			{
				DataPoint element = queue.poll(1, SECONDS);
				if (element != null)
					result.add(element);
			}
			catch (InterruptedException e)
			{
				close();
				Thread.currentThread().interrupt();
			}

		if (lastException != null)
			throw lastException;
		
		return result;
	}

	public Map<String, List<Object>> more()
	{
		return more(20);
	}

	public Map<String, List<Object>> more(int size)
	{
		List<DataPoint> datapoints = moreDataPoints(size);

		Map<String, List<Object>> result = dataStructure.stream()
				.map(DataStructureComponent::getName)
				.collect(toConcurrentMap(identity(), c -> new ArrayList<>(Arrays.asList(new Object[datapoints.size()]))));

		for (DataStructureComponent<?, ?, ?> c: dataStructure)
			for (int i = 0; i < datapoints.size(); i++)
			{
				Comparable<?> value = datapoints.get(i).get(c).get();
				if (value instanceof PeriodHolder){
					// period is just cast to string for now. Users will have the responsibility to cast it to the 
					// suitable time structure in R
					value = value.toString();
				}
				if (value instanceof DateHolder || value instanceof PeriodHolder){
					// dates are cast to string in standard date format and will be cast back to Date in R
					value = value.toString();
					if(i == 0){
						toBeCast.put(c.getName(), "date");
					}
				}
				else if(value instanceof Boolean){
					// booleans are cast to integer because jri does not manage nulls correctly
					// they will be cast back to logical in R 
					value = new Integer(((Boolean)value) ? 1 : 0);
					if(i == 0){
						toBeCast.put(c.getName(), "boolean");
					}
				}
				result.get(c.getName()).set(i, value);
			}
		
		//result.values().removeIf(l -> Utils.getStream(l).allMatch(Objects::isNull));

		return result;
	}
	
	public boolean isToBeCast(){
		return toBeCast != null;
	}
	
	public Map<String, String> getToBeCast(){
		return toBeCast;
	}
}
