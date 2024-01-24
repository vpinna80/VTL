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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.util.SerBiPredicate.not;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.joining;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.Utils;

public class InclusionTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> BOOL_VAR = DataStructureComponentImpl.of("bool_var", Measure.class, BOOLEANDS);

	public static enum InOperator implements SerBiPredicate<Set<? extends ScalarValue<?, ?, ?, ?>>, ScalarValue<?, ?, ?, ?>>
	{
		IN("in", Set::contains),
		NOTIN("not_in", not(Set::contains));

		private final String text;
		private final SerBiPredicate<Set<? extends ScalarValue<?, ?, ?, ?>>, ScalarValue<?, ?, ?, ?>> test;

		private InOperator(String text, SerBiPredicate<Set<? extends ScalarValue<?, ?, ?, ?>>, ScalarValue<?, ?, ?, ?>> test)
		{
			this.text = text;
			this.test = test;
		}

		@Override
		public boolean test(Set<? extends ScalarValue<?, ?, ?, ?>> t, ScalarValue<?, ?, ?, ?> u)
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
	private final Set<ScalarValue<?, ?, ?, ?>> set;

	public InclusionTransformation(InOperator operator, Transformation operand, List<ScalarValue<?, ?, ?, ?>> list)
	{
		super(operand);
		this.operator = operator;
		this.set = new HashSet<>(list);
	}

	public InclusionTransformation(InOperator operator, Transformation operand, String dname) throws ClassNotFoundException
	{
		super(operand);
		this.operator = operator;
		EnumeratedDomainSubset<?, ?, ?, ?> domain = (EnumeratedDomainSubset<?, ?, ?, ?>) ConfigurationManager.getDefault().getMetadataRepository().getDomain(dname);
		this.set = new HashSet<>(domain.getCodeItems());
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return BooleanValue.of(operator.test(set, scalar));
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		DataStructureComponent<? extends Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();
		
		return dataset.mapKeepingKeys((DataSetMetadata) metadata, dp -> LineageNode.of(this, dp.getLineage()), 
				dp -> singletonMap(BOOL_VAR, BooleanValue.of(operator.test(set, dp.get(measure)))));
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata value = operand.getMetadata(session);

		ScalarValue<?, ?, ?, ?> item1 = null;
		for (ScalarValue<?, ?, ?, ?> item: set)
			if (item1 == null)
				item1 = item;
			else
				if (!item1.getDomain().equals(item.getDomain()))
					throw new VTLIncompatibleTypesException(operator.toString().toLowerCase(), item1, item);
		
		if (item1 == null)
			throw new IllegalStateException("At least one item is expected for " + operator.toString().toLowerCase());
		
		if (value instanceof DataSetMetadata)
		{
			DataSetMetadata ds = (DataSetMetadata) value;

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = ds.getMeasures();

			if (measures.size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, measures);
			
			DataStructureComponent<? extends Measure, ?, ?> measure = measures.iterator().next();
			if (!item1.getDomain().isAssignableFrom(measure.getDomain()))
				if (measure.getDomain().isAssignableFrom(item1.getDomain()))
				{
					// (try to) cast all items to the measure domain
					Set<? extends ScalarValue<?, ?, ?, ?>> set2 = Utils.getStream(set)
						.map(((ValueDomainSubset<?, ?>) measure.getDomain())::cast)
						.collect(toSet());
					set.clear();
					set.addAll(set2);			
				}
				else
					throw new VTLIncompatibleTypesException(operator.toString().toLowerCase(), measure, item1.getDomain());
			
			
			return new DataStructureBuilder()
					.addComponents(ds.getIDs())
					.addComponent(BOOL_VAR)
					.build();
		}
		else
			return BOOLEAN;
	}

	@Override
	public String toString()
	{
		return operand + " " + operator + set.stream().map(Object::toString).collect(joining(", ", " {", "}"));
	}
}
