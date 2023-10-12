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

import java.util.Arrays;
import java.util.OptionalDouble;

public class SerDoubleSumAvgCount implements SerDoubleConsumer
{
	private static final long serialVersionUID = 1L;

	private long count;
	private final double[] sums;

    public SerDoubleSumAvgCount()
    { 
    	this.sums = new double[3];
    }

    public SerDoubleSumAvgCount(long count, double[] sums)
	{
    	this.count = count;
		this.sums = Arrays.copyOf(sums, 3);
	}

	@Override
    public void accept(double value)
    {
        ++count;
        sums[2] += value;
        sumWithCompensation(value);
    }

    public SerDoubleSumAvgCount combine(SerDoubleSumAvgCount other)
    {
    	SerDoubleSumAvgCount comb = new SerDoubleSumAvgCount(count + other.count, sums);
        
        comb.sums[2] += other.sums[2];
        comb.sumWithCompensation(other.sums[0]);
        comb.sumWithCompensation(-other.sums[1]);
        return comb;
    }

    private void sumWithCompensation(double value)
    {
        double tmp = value - sums[1];
        double velvel = sums[0] + tmp;
        sums[1] = (velvel - sums[0]) - tmp;
        sums[0] = velvel;
    }

    public final long getCount()
    {
        return count;
    }

    public final OptionalDouble getSum()
    {
    	if (getCount() < 0)
    		return OptionalDouble.empty();
    	
        return OptionalDouble.of(internalSum());
    }

	private double internalSum()
	{
		double tmp =  sums[0] - sums[1];
        if (Double.isNaN(tmp) && Double.isInfinite(sums[2]))
            return sums[2];
        else
            return tmp;
	}

    public final OptionalDouble getAverage()
    {
        return getCount() > 0 ? OptionalDouble.of(internalSum() / getCount()) : OptionalDouble.empty();
    }
}
