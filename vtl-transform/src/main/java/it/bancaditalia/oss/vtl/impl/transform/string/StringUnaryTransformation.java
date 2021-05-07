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
package it.bancaditalia.oss.vtl.impl.transform.string;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.util.stream.Collectors.toSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
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
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class StringUnaryTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;

	public enum StringOperator implements UnaryOperator<ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain>>
	{
		TRIM("TRIM", String::trim, StringEnumeratedDomainSubset::trim),
		LTRIM("LTRIM", s -> s.replaceAll("^\\s+",""), StringEnumeratedDomainSubset::ltrim),
		RTRIM("RTRIM", s -> s.replaceAll("\\s+$",""), StringEnumeratedDomainSubset::rtrim),
		UCASE("UCASE", String::toUpperCase, StringEnumeratedDomainSubset::ucase),
		LCASE("LCASE", String::toLowerCase, StringEnumeratedDomainSubset::lcase);

		private final String name;
		private final UnaryOperator<StringEnumeratedDomainSubset> codeListMapper;
		private final UnaryOperator<ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain>> function;

		private StringOperator(String name, UnaryOperator<String> function, UnaryOperator<StringEnumeratedDomainSubset> codeListMapper)
		{
			this.name = name;
			this.codeListMapper = codeListMapper;
			this.function = s -> s instanceof NullValue ? NullValue.instance(STRINGDS) : ((StringValue<?, ?>) s).map(function);
		}
		
		@Override
		public String toString()
		{
			return name;
		}
		
		public UnaryOperator<StringEnumeratedDomainSubset> getCodeListMapper()
		{
			return codeListMapper;
		}

		@Override
		public ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> apply(ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> t)
		{
			return function.apply(t);
		}
	}

	private final StringOperator operator;
	
	public StringUnaryTransformation(StringOperator operator, Transformation operand)
	{
		super(operand);
		
		this.operator = operator;
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return scalar.getDomain().cast(operator.apply(STRINGDS.cast(scalar)));
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		Set<DataStructureComponent<Measure, ?, ?>> components = dataset.getComponents(Measure.class);
		
		return dataset.mapKeepingKeys((DataSetMetadata) metadata, dp -> LineageNode.of(this, dp.getLineage()), dp -> {
					Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(components, Measure.class));
					map.replaceAll((c, v) -> c.getDomain().cast(operator.apply(STRINGDS.cast(v))));
					return map;
				});
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session);
		
		if (meta instanceof ScalarValueMetadata)
			if (STRINGDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) meta).getDomain()))
				return STRING;
			else
				throw new VTLIncompatibleTypesException(operator.toString(), STRINGDS, ((ScalarValueMetadata<?, ?>) meta).getDomain());
		else
		{
			DataSetMetadata dataset = (DataSetMetadata) meta;
			
			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> nonstring = dataset.getComponents(Measure.class);
			if (dataset.getComponents(Measure.class).size() == 0)
				throw new UnsupportedOperationException("Expected at least 1 measure but found none.");
			
			nonstring.removeAll(dataset.getComponents(Measure.class, STRINGDS));
			if (nonstring.size() > 0)
				throw new UnsupportedOperationException("Expected only string measures but found: " + nonstring);
			
			Set<DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getComponents(Measure.class).stream()
					.map(m -> m.getDomain() instanceof StringEnumeratedDomainSubset
							? new DataStructureComponentImpl<>(m.getName(), Measure.class, operator.getCodeListMapper().apply((StringEnumeratedDomainSubset) m.getDomain()))
							: m
					).collect(toSet());
			
			Set<DataStructureComponent<?, ?, ?>> components = new HashSet<>(dataset);
			components.removeAll(dataset.getComponents(Measure.class));
			components.addAll(measures);
			
			return new DataStructureBuilder(components).build();
		}
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
		if (!(obj instanceof StringUnaryTransformation)) return false;
		StringUnaryTransformation other = (StringUnaryTransformation) obj;
		if (operator != other.operator) return false;
		return true;
	}
}
