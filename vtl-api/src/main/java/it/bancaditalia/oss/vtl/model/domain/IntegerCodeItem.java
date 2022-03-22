package it.bancaditalia.oss.vtl.model.domain;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.CodeItem;

/**
 * A {@link CodeItem} having a Integer value that is allowed in an {@link IntegerEnumeratedDomainSubset}.
 * 
 * @author Valentino Pinna
 */
public interface IntegerCodeItem<C extends IntegerCodeItem<C, R, S, I>, R extends Comparable<?> & Serializable, S extends IntegerEnumeratedDomainSubset<S, I, C, R>, I extends IntegerDomainSubset<I>> extends CodeItem<C, R, S, I, IntegerDomain>
{

}