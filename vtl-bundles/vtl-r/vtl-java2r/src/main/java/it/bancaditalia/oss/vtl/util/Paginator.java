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

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;

public class Paginator
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Paginator.class);
	private static final LocalDate R_EPOCH_DATE = LocalDate.of(1970, 1, 1);
	private static final double R_DOUBLE_NA = Double.longBitsToDouble(0x7ff00000000007a2L);
	private static final int R_INT_NA = Integer.MIN_VALUE;
	
	private final DataSetMetadata dataStructure;
	private final ArrayBlockingQueue<DataPoint> queue;
	private final DataStructureComponent<?, ?, ?>[] comps;
	private final int[] types;
	private final int size;
	private final Object[] result;
	
	private boolean closed = false;
	
	public Paginator(DataSet dataset, int size)
	{
		this.size = size;
		dataStructure = dataset.getMetadata();
		comps = dataStructure.stream().toArray(DataStructureComponent<?, ?, ?>[]::new);
		result = new Object[comps.length];
		types = new int[comps.length];
		queue = new ArrayBlockingQueue<>(size);
		
		for (int i = 0; i < comps.length; i++)
		{
			if (comps[i].getVariable().getDomain() instanceof NumberDomain)
				types[i] = 1;
			else if (comps[i].getVariable().getDomain() instanceof BooleanDomain)
				types[i] = 2;
			else if (comps[i].getVariable().getDomain() instanceof DateDomain)
				types[i] = 3;
			else // StringDomain, TimeDomain, TimePeriodDomain
				types[i] = 4;
		}

		Thread t = new Thread(() -> {
			try (Stream<DataPoint> stream = dataset.stream().onClose(() -> closed = true))
			{
				stream.forEach(dp -> {
					try
					{
						if (!closed)
							queue.put(dp);
					}
					catch (InterruptedException e)
					{
						closed = true;
					}
				});
			}
			finally
			{
				closed = true;
			}
		}, "Paginator@" + hashCode());
		t.setDaemon(true);
		t.start();
	}

	public DataSetMetadata getDataStructure()
	{
		return dataStructure;
	}

	public int getType(int i)
	{
		return types[i];
	}

	public String getName(int i)
	{
		return comps[i].getVariable().getName();
	}

	public int[] getIntColumn(int i)
	{
		return (int[]) result[i];
	}
	
	public double[] getDoubleColumn(int i)
	{
		return (double[]) result[i];
	}
	
	public String[] getStringColumn(int i)
	{
		return (String[]) result[i];
	}
	
	public void prepareMore()
	{
		ArrayList<DataPoint> dps = new ArrayList<>(size);
		queue.drainTo(dps);
		while (!closed && dps.size() == 0)
		{
			try
			{
				Thread.sleep(500);
				queue.drainTo(dps);
			}
			catch (InterruptedException e)
			{
				break;
			}
		}
		
		int newSize = dps.size();
		
		LOGGER.info("Retrieving {} rows from dataset.", newSize);

		boolean test = result[0] == null;
		if (!test)
			switch (types[0])
			{
				case 1: test = ((double[]) result[0]).length != newSize; break;
				case 2: 
				case 3: test = ((int[]) result[0]).length != newSize; break;
				case 4: test = ((String[]) result[0]).length != newSize; break;
			}
		if (test)
			for (int i = 0; i < comps.length; i++)
				switch (types[i])
				{
					case 1: result[i] = new double[newSize]; break;
					case 2: result[i] = new int[newSize]; break;
					case 3: result[i] = new int[newSize]; break;
					case 4: result[i] = new String[newSize]; break;
					default: throw new IllegalStateException();
				}
		
		for (int i = 0; i < result.length; i++)
		{
			Object array = result[i];
			for (int j = 0; j < newSize; j++)
			{
				Serializable value = dps.get(j).get(comps[i]).get();
				switch (types[i])
				{
					case 1: ((double[]) array)[j] = value == null ? R_DOUBLE_NA : ((Number) value).doubleValue(); break;
					case 2: ((int[]) array)[j] = value == null ? R_INT_NA : value == Boolean.TRUE ? 1 : 0; break;
					case 3: ((int[]) array)[j] = value == null ? R_INT_NA : (int) DAYS.between(R_EPOCH_DATE, (LocalDate) value); break;
					case 4: ((String[]) array)[j] = value == null ? null : value.toString(); break;
				}
			}		
		}
		
		LOGGER.debug("Retrieving {} rows from dataset finished.", newSize);
	}
}
