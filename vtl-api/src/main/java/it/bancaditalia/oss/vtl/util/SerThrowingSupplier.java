package it.bancaditalia.oss.vtl.util;

@FunctionalInterface
public interface SerThrowingSupplier<C, T extends Throwable>
{
	public C get() throws T;
}
