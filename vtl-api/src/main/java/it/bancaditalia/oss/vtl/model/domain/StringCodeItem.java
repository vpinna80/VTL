package it.bancaditalia.oss.vtl.model.domain;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.CodeItem;

/**
 * A {@link CodeItem} having a String value that is allowed in a {@link StringEnumeratedDomainSubset}.
 * 
 * @author Valentino Pinna
 */
public interface StringCodeItem<C extends StringCodeItem<C, R, S, I>, R extends Comparable<?> & Serializable, S extends StringEnumeratedDomainSubset<S, I, C, R>, I extends StringDomainSubset<I>> extends CodeItem<C, R, S, I, StringDomain>
{

}