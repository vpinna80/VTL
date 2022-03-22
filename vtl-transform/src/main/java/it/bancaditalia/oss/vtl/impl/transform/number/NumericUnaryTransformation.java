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
package it.bancaditalia.oss.vtl.impl.transform.number;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static java.util.function.DoubleUnaryOperator.identity;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;
import java.util.function.LongFunction;
import java.util.function.UnaryOperator;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class NumericUnaryTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;

	public enum NumericOperator implements UnaryOperator<ScalarValue<?, ?, ? extends NumberDomainSubset<?, ?>, ? extends NumberDomain>>
	{
		CEIL("ceil", Math::ceil, l -> l),
		FLOOR("floor", Math::floor, l -> l),
		ABS("abs", Math::abs, l -> l > 0 ? l : -l),
		EXP("exp", Math::exp, Math::exp),
		LN("ln", Math::log, Math::log),
		SQRT("sqrt", Math::sqrt, Math::sqrt),
		UNARY_PLUS("+", identity(), l -> l),
		UNARY_MINUS("-", d -> -d, l -> -l);

		private final DoubleUnaryOperator doubleOp;
		private final LongFunction<Number> longOp;
		private final String name;

		private NumericOperator(String name, DoubleUnaryOperator doubleOp, LongFunction<Number> longOp)
		{
			this.name = name;
			this.doubleOp = doubleOp; 
			this.longOp = longOp; 
		}

		@Override
		public ScalarValue<?, ?, ? extends NumberDomainSubset<?, ?>, ? extends NumberDomain> apply(ScalarValue<?, ?, ? extends NumberDomainSubset<?, ?>, ? extends NumberDomain> number)
		{
			if (number instanceof NullValue)
				return NullValue.instance(NUMBERDS);
			if (number instanceof IntegerValue)
			{
				Number res = longOp.apply(((IntegerValue<?, ?>) number).get());
				return res instanceof Long ? IntegerValue.of(res.longValue()) : DoubleValue.of(res.doubleValue());
			}
			else
				return DoubleValue.of(doubleOp.applyAsDouble(((DoubleValue<?>) number).get().doubleValue()));
		}
		
		public String capsize(Transformation operand)
		{
			if (this == UNARY_PLUS || this == UNARY_MINUS)
				return name + operand.toString();
			else
				return name + "(" + operand.toString() + ")";
		}
	}

	private final NumericOperator operator;

	public NumericUnaryTransformation(NumericOperator function, Transformation operand)
	{
		super(operand);

		this.operator = function;
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return operator.apply(NUMBERDS.cast(scalar));
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		Set<DataStructureComponent<Measure, ?, ?>> components = dataset.getComponents(Measure.class);
		
		return dataset.mapKeepingKeys(dataset.getMetadata(), dp -> LineageNode.of(this, dp.getLineage()), dp -> {
					Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(components, Measure.class));
					map.replaceAll((c, v) -> operator.apply(NUMBERDS.cast(v)));
					return map;
				});
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session);
		
		if (meta instanceof ScalarValueMetadata)
			if (NUMBER.isAssignableFrom(((ScalarValueMetadata<?, ?>) meta).getDomain()))
				return NUMBER;
			else
				throw new VTLIncompatibleTypesException(operator.toString(), NUMBERDS, ((ScalarValueMetadata<?, ?>) meta).getDomain());
		else
		{
			DataSetMetadata dataset = (DataSetMetadata) meta;
			
			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> nonnumeric = dataset.getComponents(Measure.class);
			if (dataset.getComponents(Measure.class).size() == 0)
				throw new UnsupportedOperationException("Expected at least 1 measure but found none.");
			
			nonnumeric.removeAll(dataset.getComponents(Measure.class, NUMBERDS));
			if (nonnumeric.size() > 0)
				throw new UnsupportedOperationException("Expected only numeric measures but found: " + nonnumeric);
			
			return dataset;
		}
	}
	
	@Override
	public String toString()
	{
		return operator.capsize(operand);
	}
}
