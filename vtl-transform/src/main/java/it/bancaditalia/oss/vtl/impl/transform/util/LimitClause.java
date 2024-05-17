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
package it.bancaditalia.oss.vtl.impl.transform.util;

import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.FOLLOWING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.PRECEDING;

import java.io.Serializable;
import java.security.InvalidParameterException;

import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion;

public class LimitClause implements LimitCriterion, Serializable
{
	public static final LimitClause UNBOUNDED_PRECEDING = new LimitClause(PRECEDING, Integer.MAX_VALUE);
	public static final LimitClause UNBOUNDED_FOLLOWING = new LimitClause(FOLLOWING, Integer.MAX_VALUE);
	public static final LimitClause CURRENT_DATA_POINT = new LimitClause(FOLLOWING, 0);
	
	private static final long serialVersionUID = 1L;
	private final LimitDirection direction;
	private final int count;
	
	public static final LimitClause following(int count)
	{
		return new LimitClause(FOLLOWING, count);
	}
	
	public static final LimitClause preceding(int count)
	{
		return new LimitClause(PRECEDING, count);
	}	
	
	public LimitClause(LimitDirection direction, IntegerValue<?, ?> limitCount)
	{
		this(direction, limitCount.get().intValue());
	}

	public LimitClause(LimitDirection direction, int count)
	{
		if (count < 0)
			throw new InvalidParameterException("In a window clause of an analytic invocation, limit count must be non-negative, but it is " + count);
		
		this.direction = direction;
		this.count = count;
	}

	@Override
	public LimitDirection getDirection()
	{
		return direction;
	}

	@Override
	public int getCount()
	{
		return count;
	}
	
	@Override
	public String toString()
	{
		if (count == 0)
			return "current data point";
		else if (count == Long.MAX_VALUE)
			return "unbounded " + direction.toString().toLowerCase();
		else
			return count + " " + direction.toString().toLowerCase();
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (count ^ (count >>> 32));
		result = prime * result + ((direction == null) ? 0 : direction.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LimitClause other = (LimitClause) obj;
		if (count != other.count)
			return false;
		if (direction != other.direction)
			return false;
		return true;
	}
}