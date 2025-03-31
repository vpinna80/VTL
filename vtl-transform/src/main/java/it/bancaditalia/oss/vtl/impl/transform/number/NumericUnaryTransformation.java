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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.isUseBigDecimal;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static java.util.function.DoubleUnaryOperator.identity;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;
import java.util.function.LongFunction;
import java.util.function.UnaryOperator;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
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
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class NumericUnaryTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;

	public enum NumericOperator implements UnaryOperator<ScalarValue<?, ?, ?, ?>>
	{
		CEIL("ceil", Math::ceil, bd -> bd.setScale(0, RoundingMode.CEILING), l -> l),
		FLOOR("floor", Math::floor, bd -> bd.setScale(0, RoundingMode.FLOOR), l -> l),
		ABS("abs", Math::abs, BigDecimal::abs, l -> l > 0 ? l : -l),
		EXP("exp", Math::exp, bd -> new BigDecimal(Math.exp(bd.doubleValue())), Math::exp),
		LN("ln", Math::log, bd -> new BigDecimal(Math.log(bd.doubleValue())), Math::log),
		SQRT("sqrt", Math::sqrt, bd -> new BigDecimal(Math.sqrt(bd.doubleValue())), Math::sqrt),
		UNARY_PLUS("+", identity(), SerUnaryOperator.identity(), l -> l),
		UNARY_MINUS("-", d -> -d, BigDecimal::negate, l -> -l);

		private final DoubleUnaryOperator doubleOp;
		private final SerUnaryOperator<BigDecimal> bigdOp;
		private final LongFunction<Number> longOp;
		private final String name;

		private NumericOperator(String name, DoubleUnaryOperator doubleOp, SerUnaryOperator<BigDecimal> bigdOp, LongFunction<Number> longOp)
		{
			this.name = name;
			this.doubleOp = doubleOp;
			this.bigdOp = bigdOp; 
			this.longOp = longOp; 
		}

		@Override
		public ScalarValue<?, ?, ? extends NumberDomainSubset<?, ?>, ? extends NumberDomain> apply(ScalarValue<?, ?, ?, ?> number)
		{
			if (number.isNull())
				return NullValue.instance(NUMBERDS);
			if (number instanceof IntegerValue)
			{
				Number res = longOp.apply(((IntegerValue<?, ?>) number).get());
				return res instanceof Long ? IntegerValue.of(res.longValue()) : createNumberValue(res);
			}
			else if (isUseBigDecimal())
				return createNumberValue(bigdOp.apply(((BigDecimalValue<?>) number).get()));
			else
				return createNumberValue(doubleOp.applyAsDouble(((DoubleValue<?>) number).get().doubleValue()));
		}
		
		public String capsizeString(Transformation operand)
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
	protected VTLValue evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return operator.apply(scalar);
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata)
	{
		return dataset.mapKeepingKeys((DataSetMetadata) metadata, lineageEnricher(this), dp -> {
				Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(Measure.class));
				map.replaceAll((c, v) -> operator.apply(v));
				return map;
			});
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session);
		
		if (!meta.isDataSet())
			if (NUMBER.isAssignableFrom(((ScalarValueMetadata<?, ?>) meta).getDomain()))
				return NUMBER;
			else
				throw new VTLIncompatibleTypesException(operator.toString(), NUMBERDS, ((ScalarValueMetadata<?, ?>) meta).getDomain());
		else
		{
			DataSetMetadata dataset = (DataSetMetadata) meta;
			
			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> nonnumeric = new HashSet<>(dataset.getMeasures());
			if (dataset.getMeasures().size() == 0)
				throw new UnsupportedOperationException("Expected at least 1 measure but found none.");
			
			nonnumeric.removeAll(dataset.getComponents(Measure.class, NUMBERDS));
			if (nonnumeric.size() > 0) 
				throw new UnsupportedOperationException("Expected only numeric measures but found: " + nonnumeric);
			
			return new DataStructureBuilder(dataset).removeComponents(dataset.getComponents(Attribute.class)).build();
		}
	}
	
	@Override
	public String toString()
	{
		return operator.capsizeString(operand);
	}
}
