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

import static it.bancaditalia.oss.vtl.impl.transform.util.WindowView.WindowDirection.FOLLOWING;
import static it.bancaditalia.oss.vtl.impl.transform.util.WindowView.WindowDirection.PRECEDING;
import static it.bancaditalia.oss.vtl.impl.transform.util.WindowView.WindowRangeType.DATAPOINTS;
import static it.bancaditalia.oss.vtl.impl.transform.util.WindowView.WindowRangeType.RANGE;
import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.Utils;

public class WindowView
{
	public static final WindowClause UNBOUNDED_PRECEDING_TO_CURRENT_DATA_POINT = 
			new WindowClause(DATAPOINTS, new LimitClause(PRECEDING, Integer.MAX_VALUE), new LimitClause(FOLLOWING, 0));
	
	public enum WindowRangeType
	{
		RANGE, DATAPOINTS
	}
	
	public enum WindowDirection
	{
		PRECEDING, FOLLOWING
	}

	public static class WindowClause implements Serializable
	{
		private static final long serialVersionUID = 1L;

		private final WindowRangeType rangeType;
		private final LimitClause preceding;
		private final LimitClause following;

		public WindowClause(WindowRangeType rangeType, LimitClause preceding, LimitClause following)
		{
			if (rangeType == RANGE)
				throw new UnsupportedOperationException("range sliding windows are not implemented in analytic invocation");

			this.rangeType = rangeType;
			this.preceding = preceding;
			this.following = following;
		}

		public LimitClause getPreceding()
		{
			return preceding;
		}

		public LimitClause getFollowing()
		{
			return following;
		}
		
		public WindowRangeType getRangeType()
		{
			return rangeType;
		}
		
		@Override
		public String toString()
		{
			return (rangeType == RANGE ? "range" : "data points") + " between " + preceding + " and " + following;
		}
	}
	
	public static class LimitClause implements Serializable
	{
		private static final long serialVersionUID = 1L;
		private final WindowDirection direction;
		private final Integer count;
		
		public LimitClause(WindowDirection direction, IntegerValue<?> limitCount)
		{
			this(direction, limitCount.get().intValue());
		}

		private LimitClause(WindowDirection direction, int count)
		{
			if (count < 0)
				throw new InvalidParameterException("In a window clause of an analytic invocation, limit count must be non-negative, but it is " + count);
			
			this.direction = direction;
			this.count = count;
		}

		public WindowDirection getDirection()
		{
			return direction;
		}

		public Integer getCount()
		{
			return count;
		}
		
		@Override
		public String toString()
		{
			if (count == 0)
				return "current data point";
			else if (count == Integer.MAX_VALUE)
				return "unbounded " + direction.toString().toLowerCase();
			else
				return count + " " + direction.toString().toLowerCase();
		}
	}
	
	private final Stream<Entry<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, List<DataPoint>>> windows;

	public WindowView(NavigableSet<DataPoint> ordered, WindowClause windowClause)
	{
		int preceding = windowClause.getPreceding().getCount();
		int following = windowClause.getFollowing().getCount();
				
		windows = Utils.getStream(ordered)
				.map(dp -> {
					final SortedSet<DataPoint> precedingSet = ordered.headSet(dp);
					final int skipHeadLength = precedingSet.size() < preceding ? 0 : precedingSet.size() - preceding;
					return new SimpleEntry<>(dp.getValues(Identifier.class), Stream.concat(
							precedingSet.stream().skip(skipHeadLength),
							ordered.tailSet(dp).stream().limit(following + 1)
						).collect(toList()));
				});
	}
	
	public Stream<Entry<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, List<DataPoint>>> getWindows()
	{
		return windows;
	}
}
