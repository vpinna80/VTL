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
package it.bancaditalia.oss.vtl.impl.transform.bool;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.util.Utils.not;
import static java.util.Collections.singletonMap;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class InclusionTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;

	public static enum InOperator implements BiPredicate<Set<? extends ScalarValue<?, ?, ?>>, ScalarValue<?, ?, ?>>
	{
		IN("in", Set::contains),
		NOTIN("not_in", not(Set::contains));

		private final String text;
		private final BiPredicate<Set<? extends ScalarValue<?, ?, ?>>, ScalarValue<?, ?, ?>> test;

		private InOperator(String text, BiPredicate<Set<? extends ScalarValue<?, ?, ?>>, ScalarValue<?, ?, ?>> test)
		{
			this.text = text;
			this.test = test;
		}

		@Override
		public boolean test(Set<? extends ScalarValue<?, ?, ?>> t, ScalarValue<?, ?, ?> u)
		{
			return test.test(t,  u);
		}
		
		@Override
		public String toString()
		{
			return text;
		}
	}

	private final InOperator operator;
	private final Set<? extends ScalarValue<?, ?, ?>> set;

	private DataSetMetadata    metadata = null;

	public InclusionTransformation(InOperator operator, Transformation operand, List<ScalarValue<?, ?, ?>> list)
	{
		super(operand);
		this.operator = operator;
		this.set = new HashSet<>(list);
	}

	public InclusionTransformation(InOperator operator, Transformation operand, String dname)
	{
		super(operand);
		this.operator = operator;
		StringEnumeratedDomainSubset domain = (StringEnumeratedDomainSubset) ConfigurationManager.getDefault().getMetadataRepository().getDomain(dname);
		this.set = domain.getCodeItems();
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?> scalar)
	{
		return BooleanValue.of(operator.test(set, scalar));
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
	{
		DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> resultMeasure = metadata.getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, ?, ?> datasetMeasure = dataset.getComponents(Measure.class).iterator().next();
		
		return dataset.mapKeepingKeys(metadata, dp -> singletonMap(resultMeasure, BooleanValue.of(operator.test(set, dp.get(datasetMeasure)))));
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata value = operand.getMetadata(session);

		if (value instanceof DataSetMetadata)
		{
			DataSetMetadata ds = (DataSetMetadata) value;

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = ds.getComponents(Measure.class);

			if (measures.size() != 1)
				throw new UnsupportedOperationException("Expected single measure but found: " + measures);

			return metadata = new DataStructureBuilder()
					.addComponents(ds.getComponents(Identifier.class))
					.addComponent(new DataStructureComponentImpl<>("bool_var", Measure.class, BOOLEANDS))
					.build();
		}
		else
		{
			return (ScalarValueMetadata<BooleanDomainSubset>) () -> BOOLEANDS;
		}
	}

	@Override
	public String toString()
	{
		return operand + " " + operator + set.stream().map(Object::toString).collect(Collectors.joining(", ", " {", "}"));
	}
}
