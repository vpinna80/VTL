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
package it.bancaditalia.oss.vtl.util;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.BinaryOperator;

/**
 * Simplified version of AtomicReference, to be used with Spark 
 * serialization mechanism to store an instance of a unknown class V.
 * 
 * @param <V> The reference type class descriptor. It should not be 
 * a class with a wildcard generic parameter, i.e. List$lt;?&gt;.
 */
public class Holder<V> implements Serializable
{
    private static final long serialVersionUID = 1L;
    private static final VarHandle VALUE;

    static {
        try
        {
            MethodHandles.Lookup l = MethodHandles.lookup();
            VALUE = l.findVarHandle(Holder.class, "value", Object.class);
        }
        catch (ReflectiveOperationException e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    public final Class<?> repr;
    @SuppressWarnings("unused") // used by the VarHandle
	private V value;

    public Holder(Class<?> repr)
    {
		this.repr = requireNonNull(repr);
    }

    public V get()
    {
        return (V) VALUE.getVolatile(this);
    }

    public void set(V newValue)
    {
        VALUE.setVolatile(this, newValue);
    }

    public boolean compareAndSet(V expectedValue, V newValue)
    {
        return VALUE.compareAndSet(this, expectedValue, newValue);
    }

    public V accumulateAndGet(V x, BinaryOperator<V> accumulatorFunction)
    {
        V prev = get(), next = null;
        for (boolean haveNext = false;;)
        {
            if (!haveNext)
                next = accumulatorFunction.apply(prev, x);
            if (VALUE.weakCompareAndSet(this, prev, next))
                return next;
            haveNext = (prev == (prev = get()));
        }
    }

    public String toString()
    {
        return String.valueOf(get());
    }
}
