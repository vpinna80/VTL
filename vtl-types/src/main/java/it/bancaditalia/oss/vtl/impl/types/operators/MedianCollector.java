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
package it.bancaditalia.oss.vtl.impl.types.operators;

import static java.util.Collections.reverseOrder;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Optional;
import java.util.PriorityQueue;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class MedianCollector extends SerCollector<ScalarValue<?, ?, ?, ?>, MedianCollector.MedianAcc, Optional<ScalarValue<?, ?, ?, ?>>>
{
	private static final long serialVersionUID = 1L;

	public static class MedianAcc implements Serializable
	{
		private static final long serialVersionUID = 1L;

		private final Class<?> repr;
		private PriorityQueue<ScalarValue<?, ?, ?, ?>> left = new PriorityQueue<>(reverseOrder());
		private PriorityQueue<ScalarValue<?, ?, ?, ?>> right = new PriorityQueue<>();

		public MedianAcc(Class<?> repr)
		{
			this.repr = repr;
		}

		public void accumulate(ScalarValue<?, ?, ?, ?> value)
		{
			if (left.isEmpty() || value.compareTo(left.peek()) <= 0)
			{
				left.add(value);
				if (left.size() > right.size() + 1)
					right.add(left.poll());
			}
			else
			{
				right.add(value);
				if (right.size() > left.size())
					left.add(right.poll());
			}
		}

		public MedianAcc merge(MedianAcc other)
		{
			left.addAll(other.left);
			right.addAll(other.right);

			while (left.size() > right.size() + 1)
				right.add(left.poll());
			while (right.size() > left.size())
				left.add(right.poll());

			return this;
		}

		public Optional<ScalarValue<?, ?, ?, ?>> finish()
		{
			while (left.size() > right.size() + 1)
				right.add(left.poll());
			while (right.size() > left.size())
				left.add(right.poll());
			
			ScalarValue<?, ?, ?, ?> l = left.peek();
			ScalarValue<?, ?, ?, ?> r = right.peek();
			
			if (l != null && r != null)
				return Optional.of(l.compareTo(r) <= 0 ? l : r);
			else
				return Optional.ofNullable(l);
		}

		public Class<?> getRepr()
		{
			return repr;
		}
	}

	public MedianCollector(Class<?> repr)
	{
		super(() -> new MedianAcc(repr), MedianAcc::accumulate, MedianAcc::merge, MedianAcc::finish, EnumSet.noneOf(Characteristics.class));
	}
	
	public static SerCollector<ScalarValue<?, ?, ?, ?>, ?, Optional<ScalarValue<?, ?, ?, ?>>> medianCollector(Class<?> repr)
	{
		return new MedianCollector(repr); 
	}
}
