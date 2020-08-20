package it.bancaditalia.oss.vtl.model.data;

/**
 * A metadata for a {@link ScalarValue} providing information on its {@link ValueDomainSubset}.
 * 
 * @author Valentino Pinna
 *
 * @param <S> The {@link ValueDomainSubset} type.
 */
@FunctionalInterface
public interface VTLScalarValueMetadata<S extends ValueDomainSubset<?>> extends VTLValueMetadata
{
	/**
	 * @return the {@link ValueDomainSubset} instance represented by this VTLScalarValueMetadata
	 */
	public S getDomain();
}