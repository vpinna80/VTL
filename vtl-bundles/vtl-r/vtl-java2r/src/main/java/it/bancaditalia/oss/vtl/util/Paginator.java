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

import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.data.date.DayHolder;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;

public class Paginator implements AutoCloseable
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Paginator.class);
	private static final double R_DOUBLE_NA = Double.longBitsToDouble(0x7ff00000000007a2L);
	private static final int R_INT_NA = Integer.MIN_VALUE;
	
	private final BlockingQueue<DataPoint> queue = new ArrayBlockingQueue<>(100);
	private final DataSetMetadata dataStructure;

	private boolean closed = false;
	private RuntimeException lastException = null;
	
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

	public Map<String, Object> more()
	{
		return more(20);
	}

	public Map<String, Object> more(int size)
	{
		List<DataPoint> datapoints = moreDataPoints(size);
		Map<String, Object> result = new HashMap<>();

		for (DataStructureComponent<?, ?, ?> c: dataStructure)
			if (c.getDomain() instanceof NumberDomain)
			{
				double[] array = (double[]) result.computeIfAbsent(c.getName(), n -> new double[datapoints.size()]);
				for (int i = 0; i < datapoints.size(); i++)
				{
					Serializable v = datapoints.get(i).get(c).get();
					array[i] = v == null ? R_DOUBLE_NA : ((Number) v).doubleValue();
				}
			}
			else if (c.getDomain() instanceof BooleanDomain || c.getDomain() instanceof DateDomain)
			{
				int[] array = (int[]) result.computeIfAbsent(c.getName(), n -> new int[datapoints.size()]);
				for (int i = 0; i < datapoints.size(); i++)
				{
					Serializable v = datapoints.get(i).get(c).get();
					if (v == null)
						array[i] = R_INT_NA;
					else if (c.getDomain() instanceof BooleanDomain)
						array[i] = (Boolean) v ? 1 : 0;
					else
						array[i] = (int) DAYS.between(LocalDate.of(1970, 1, 1), ((DayHolder) v).getLocalDate());
				}
			}
			else if (c.getDomain() instanceof StringDomain || c.getDomain() instanceof TimePeriodDomain)
			{
				String[] array = (String[]) result.computeIfAbsent(c.getName(), n -> new String[datapoints.size()]);
				for (int i = 0; i < datapoints.size(); i++)
				{
					Serializable v = datapoints.get(i).get(c).get();
					array[i] = v == null ? null : v.toString();
				}
			}
			else
				throw new UnsupportedOperationException("Values of domain " + c.getDomain() + " cannot be exported to R.");

		return result;
	}
}
