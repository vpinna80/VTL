package it.bancaditalia.oss.vtl.impl.transform.ops;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static java.util.Collections.singletonMap;

import java.util.Set;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureImpl.Builder;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class BetweenTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> BOOL_MEASURE = new DataStructureComponentImpl<>("bool_var", Measure.class, BOOLEANDS);
	
	private final ScalarValue<?, ?, ?> from;
	private final ScalarValue<?, ?, ?> to;
	private final ValueDomainSubset<?> domain;

	private VTLDataSetMetadata metadata;

	public BetweenTransformation(Transformation operand, Transformation fromT, Transformation toT)
	{
		super(operand);
		
		if (fromT instanceof ConstantOperand && toT instanceof ConstantOperand)
		{
			this.from = ((ConstantOperand<?, ?, ?, ?>) fromT).eval(null);
			this.to = ((ConstantOperand<?, ?, ?, ?>) toT).eval(null);
		}
		else
			throw new UnsupportedOperationException("Non-constant range parameters in between expression are not supported");
		
		if (!from.getDomain().isAssignableFrom(to.getDomain()) || !to.getDomain().isAssignableFrom(from.getDomain()))
			throw new VTLIncompatibleTypesException("between", from, to);
		if (from instanceof NullValue || to instanceof NullValue)
			throw new NullPointerException("Between: Null constant not allowed.");
		this.domain = from.getDomain(); 
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata op = operand.getMetadata(scheme);

		if (op instanceof VTLDataSetMetadata)
		{
			VTLDataSetMetadata ds = (VTLDataSetMetadata) op;

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = ds.getComponents(Measure.class);

			if (measures.size() != 1)
				throw new UnsupportedOperationException("Expected single measure but found: " + measures);

			DataStructureComponent<? extends Measure, ?, ?> measure = measures.iterator().next();

			if (!measure.getDomain().isAssignableFrom(domain))
				throw new VTLIncompatibleTypesException("between", measure, domain);

			return metadata = new Builder()
					.addComponents(ds.getComponents(Identifier.class))
					.addComponent(BOOL_MEASURE)
					.build();
		}
		else
			return BOOLEAN;
	}

	@Override
	protected ScalarValue<?, ?, ?> evalOnScalar(ScalarValue<?, ?, ?> scalar)
	{
		return scalar instanceof NullValue ? NullValue.instance(BOOLEANDS) : BooleanValue.of(scalar.compareTo(from) >= 0 && scalar.compareTo(to) <= 0);
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
	{
		DataStructureComponent<? extends Measure, ?, ?> measure = dataset.getComponents(Measure.class).iterator().next();
		return dataset.mapKeepingKeys(metadata, dp -> singletonMap(BOOL_MEASURE, evalOnScalar(dp.get(measure))));
	}
	
	@Override
	public String toString()
	{
		return "between(" + operand + ", " + from + ", " + to + ")";
	}
}
