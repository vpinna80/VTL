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
package it.bancaditalia.oss.vtl.impl.transform.dataset;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.DIFF;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.SUM;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightF2DataSet;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.TriFunction;
import it.bancaditalia.oss.vtl.util.Utils;

public class DatasetUnaryTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(DatasetUnaryTransformation.class);

	private final static boolean bothIntegers(ScalarValue<?, ?, ?, ?> l, ScalarValue<?, ?, ?, ?> r)
	{
		return l instanceof IntegerValue && r instanceof IntegerValue;
	}

	public enum DatasetOperator implements TriFunction<Lineage, DataSet, DataStructureComponent<Identifier, ?, ?>, Stream<DataPoint>>
	{
		STOCK_TO_FLOW("stock_to_flow", false, (b, a) -> bothIntegers(b, a) 
				? DIFF.applyAsInt((NumberValue<?, ?, ?, ?>) a, (NumberValue<?, ?, ?, ?>) b) 
				: DIFF.applyAsNumber((NumberValue<?, ?, ?, ?>) a, (NumberValue<?, ?, ?, ?>) b)), 
		FLOW_TO_STOCK("flow_to_stock", true, (acc, v) -> bothIntegers(acc, v) 
				? SUM.applyAsInt((NumberValue<?, ?, ?, ?>) acc, (NumberValue<?, ?, ?, ?>) v) 
				: SUM.applyAsNumber((NumberValue<?, ?, ?, ?>) acc, (NumberValue<?, ?, ?, ?>) v)); 
		
		private final BinaryOperator<ScalarValue<?, ?, ?, ?>> op;
		private final String text;
		private final boolean cumulating;

		private DatasetOperator(String text, boolean cumulating, BinaryOperator<ScalarValue<?, ?, ?, ?>> op)
		{
			this.text = text;
			this.cumulating = cumulating;
			this.op = op; 
		}

		@Override
		public Stream<DataPoint> apply(Lineage lineage, DataSet ds, DataStructureComponent<Identifier, ?, ?> timeid)
		{
			final DataSetMetadata metadata = ds.getMetadata();
			Set<DataStructureComponent<Measure, ?, ?>> measures = new HashSet<>(ds.getComponents(Measure.class));
			Set<DataStructureComponent<Identifier, ?, ?>> ids = new HashSet<>(ds.getComponents(Identifier.class));
			ids.remove(timeid);

			return ds.streamByKeys(ids, toConcurrentMap(i -> i, i -> true, (a, b) -> a, () -> new ConcurrentSkipListMap<>(DataPoint.compareBy(timeid))))
				.map(Map::keySet)
				.map(group -> {
					Map<DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> acc = new ConcurrentHashMap<>();
					return group.stream().map(dp -> new DataPointBuilder(Utils.getStream(measures)
							.collect(toConcurrentMap(m -> m, m -> {
								ScalarValue<?, ?, ?, ?> v = acc.merge(m, dp.get(m), op);
								if (!cumulating)
									acc.put(m, dp.get(m));
								return v; 
							}))).addAll(dp.getValues(Identifier.class))
							.build(lineage, metadata));
				}).collect(concatenating(Utils.ORDERED));
		}
		
		@Override
		public String toString()
		{
			return text;
		}
	}

	private final DatasetOperator operator;
	private transient DataStructureComponent<Identifier, ?, ?> main;

	public DatasetUnaryTransformation(DatasetOperator operator, Transformation operand)
	{
		super(operand);
		this.operator = operator;
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		return new LightF2DataSet<>((DataSetMetadata) metadata, Utils.partial(operator::apply, getLineage()), dataset, main);
	}
	
	@Override
	protected final VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		throw new VTLInvalidParameterException(scalar, DataSet.class); 
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata metadata = operand.getMetadata(session);
		
		if (metadata instanceof DataSetMetadata)
		{
			DataSetMetadata dsmeta = (DataSetMetadata) metadata;

			if (main != null)
				return dsmeta;
			
			Set<? extends DataStructureComponent<Identifier, ?, ?>> ids = dsmeta.getComponents(Identifier.class);
			if (ids.size() == 0)
				throw new VTLMissingComponentsException("Time identifier", ids);
			
			main = dsmeta.contains("TIME_PERIOD") ? dsmeta.getComponent("TIME_PERIOD", Identifier.class).get() : ids.iterator().next(); 
			if (ids.size() > 1)
			{
				LOGGER.warn("Expected only one identifier, but found: " + ids);
				LOGGER.warn("Results may vary between executions!!!");
				LOGGER.warn(main + " will be chosen as date/time identifier.");
			}
			
			Set<? extends DataStructureComponent<Measure, ?, ?>> measures = dsmeta.getComponents(Measure.class);
			if (measures.size() == 0)
				throw new VTLMissingComponentsException("At least one numeric measure", dsmeta);
			
			for (DataStructureComponent<Measure, ?, ?> measure: measures)
				if (!NUMBERDS.isAssignableFrom(measure.getDomain()))
					throw new VTLIncompatibleTypesException(operator.toString(), NUMBERDS, measure.getDomain());

			return dsmeta;
		}
		else
			throw new VTLInvalidParameterException(metadata, DataSetMetadata.class);
	}
	
	@Override
	public String toString()
	{
		return operator + "(" + operand + ")";
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((operator == null) ? 0 : operator.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof DatasetUnaryTransformation)) return false;
		DatasetUnaryTransformation other = (DatasetUnaryTransformation) obj;
		if (operator != other.operator) return false;
		return true;
	}
}
