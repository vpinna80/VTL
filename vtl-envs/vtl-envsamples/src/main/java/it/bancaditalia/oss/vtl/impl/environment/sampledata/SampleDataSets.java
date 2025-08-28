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
package it.bancaditalia.oss.vtl.impl.environment.sampledata;

import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleValues.VAR_SAMPLE_LEN;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.IDENT_BOOLEAN_1;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.IDENT_DATE_1;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.IDENT_DATE_2;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.IDENT_DATE_3;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.IDENT_STRING_1;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.IDENT_STRING_3;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.IDENT_STRING_5;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.IDENT_STRING_6;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.IDENT_STRING_7;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_BOOLEAN_1;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_BOOLEAN_2;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_BOOLEAN_3;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_BOOLEAN_4;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_INTEGER_1;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_INTEGER_2;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_INTEGER_3;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_NUMBER_1;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_NUMBER_2;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_STRING_3;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_STRING_4;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleVariables.MEASURE_STRING_8;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.util.Utils.toEntry;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.SerTriFunction;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

public enum SampleDataSets implements DataSet
{
	SAMPLE1(IDENT_STRING_1, IDENT_BOOLEAN_1, MEASURE_NUMBER_1, MEASURE_INTEGER_1),
	SAMPLE2(IDENT_STRING_1, MEASURE_NUMBER_2, MEASURE_INTEGER_2),
	SAMPLE3(IDENT_STRING_1, MEASURE_BOOLEAN_2),
	SAMPLE4(IDENT_STRING_1, MEASURE_BOOLEAN_3),
	SAMPLE5(IDENT_STRING_1, MEASURE_INTEGER_2),
	SAMPLE6(IDENT_STRING_1, MEASURE_INTEGER_3),
	SAMPLE7(IDENT_DATE_1, IDENT_STRING_5, MEASURE_INTEGER_2),
	SAMPLE8(IDENT_DATE_2, IDENT_STRING_6, MEASURE_INTEGER_2),
	SAMPLE9(IDENT_DATE_3, IDENT_STRING_7, MEASURE_INTEGER_2),
	SAMPLE10(IDENT_STRING_3, MEASURE_INTEGER_3),
	SAMPLE11(IDENT_STRING_1, MEASURE_BOOLEAN_1),
	SAMPLE12(IDENT_STRING_1, MEASURE_BOOLEAN_4),
	SAMPLE13(IDENT_STRING_1, MEASURE_STRING_8),
	SAMPLE14(IDENT_STRING_1, MEASURE_STRING_3),
	SAMPLE15(IDENT_STRING_1, MEASURE_STRING_4),
	SAMPLE16(IDENT_STRING_1, MEASURE_NUMBER_1),
	SAMPLE17(IDENT_STRING_1, MEASURE_NUMBER_2);

	private final DataSet dataset;

	private SampleDataSets(SampleVariables... components)
	{
		dataset = createSample(components);
	}
	
	public VTLAlias getAlias()
	{
		return VTLAliasImpl.of(name());
	}

	public static DataSetStructure createStructure(SampleVariables... variables)
	{
		Map<String, AtomicInteger> counts = new HashMap<>(); 
		
		for (SampleVariables variable: variables)
			counts.put(variable.getType(), new AtomicInteger(1));
		
		return Arrays.stream(variables)
			.map(variable -> variable.getComponent(counts.get(variable.getType()).getAndIncrement()))
			.collect(DataSetStructureBuilder.toDataStructure());
	}
	
	private static DataSet createSample(SampleVariables variables[])
	{
		DataSetStructure structure = createStructure(variables);
		
		return new StreamWrapperDataSet(structure, () -> Utils.getStream(VAR_SAMPLE_LEN)
				.mapToObj(dpIdx -> {
					Map<String, AtomicInteger> counts = new HashMap<>(); 
					for (SampleVariables variable: variables)
						counts.put(variable.getType(), new AtomicInteger(1));
					
					return Arrays.stream(variables)
						.map(toEntry(
							var -> var.getComponent(counts.get(var.getType()).getAndIncrement()),
							var -> SampleValues.getValues(var.getType(), var.getIndex()).get(dpIdx) 
						)).collect(toDataPoint(LineageExternal.of("sample"), structure));
				}));
	}

	@Override
	public DataSet subspace(Map<? extends DataSetComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keyValues, SerUnaryOperator<Lineage> lineageOperator)
	{
		DataSetStructure newMetadata = new DataSetStructureBuilder(dataset.getMetadata()).removeComponents(keyValues.keySet()).build();
		
		return new AbstractDataSet(newMetadata)
		{
			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return dataset.stream()
						.filter(dp -> dp.matches(keyValues))
						.map(dp -> new DataPointBuilder(dp).delete(keyValues.keySet()).build(lineageOperator.apply(dp.getLineage()), newMetadata));
			}
		};
}
	
	public static DataSet getCustomSample(String domain, int level)
	{
		return createSample(new SampleVariables[] { IDENT_STRING_1, SampleVariables.valueOf("MEASURE_" + domain + "_" + level) });
	}

	public Stream<DataPoint> stream()
	{
		return dataset.stream();
	}

	public DataSetStructure getMetadata()
	{
		return dataset.getMetadata();
	}

	public DataSet membership(VTLAlias component, SerUnaryOperator<Lineage> lineageOp)
	{
		return dataset.membership(component, lineageOp);
	}

	public Optional<DataSetComponent<?, ?, ?>> getComponent(VTLAlias name)
	{
		return dataset.getComponent(name);
	}

	public DataSet getMatching(Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		return dataset.getMatching(keyValues);
	}

	@Override
	public DataSet filteredMappedJoin(DataSetStructure metadata, DataSet other,
		SerBinaryOperator<DataPoint> merge,
		DataSetComponent<?, ? extends BooleanDomainSubset<?>, ? extends BooleanDomain> having)
	{
		return dataset.filteredMappedJoin(metadata, other, merge, having);
	}
	
	@Override
	public VTLValue enrichLineage(SerUnaryOperator<Lineage> lineageEnricher)
	{
		return dataset.enrichLineage(lineageEnricher);
	}
	
	@Override
	public DataSet filter(SerPredicate<DataPoint> predicate, SerUnaryOperator<Lineage> lineageOperator)
	{
		return dataset.filter(predicate, lineageOperator);
	}
	
	public DataSet mapKeepingKeys(DataSetStructure metadata,
			SerUnaryOperator<Lineage> lineageOperator, SerFunction<? super DataPoint, ? extends Map<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator)
	{
		return dataset.mapKeepingKeys(metadata, lineageOperator, operator);
	}

	@Override
	public <T extends Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, TT> VTLValue aggregate(VTLValueMetadata metadata, 
			Set<DataSetComponent<Identifier, ?, ?>> keys, SerCollector<DataPoint, ?, T> groupCollector, 
			SerTriFunction<? super T, ? super List<Lineage>, ? super Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, TT> finisher)
	{
		return dataset.aggregate(metadata, keys, groupCollector, finisher);
	}

	@Override
	public <T, TT> DataSet analytic(SerUnaryOperator<Lineage> lineageOp, DataSetComponent<?, ?, ?> sourceComp, DataSetComponent<?, ?, ?> destComp, WindowClause clause,
			SerFunction<DataPoint, T> extractor, SerCollector<T, ?, TT> collector, SerBiFunction<TT, T, Collection<? extends ScalarValue<?, ?, ?, ?>>> finisher)
	{
		return dataset.analytic(lineageOp, sourceComp, destComp, clause, extractor, collector, finisher);
	}
	
	@Override
	public DataSet union(List<DataSet> others, SerUnaryOperator<Lineage> lineageOp, boolean check)
	{
		return dataset.union(others, lineageOp);
	}
	
	@Override
	public DataSet flatmapKeepingKeys(DataSetStructure metadata, SerUnaryOperator<Lineage> lineageOperator,
			SerFunction<? super DataPoint, ? extends Stream<? extends Map<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>>> operator)
	{
		throw new UnsupportedOperationException();
	}
}
