package it.bancaditalia.oss.vtl.eclipse.impl.markers;

import org.eclipse.jface.text.source.Annotation;
import org.eclipse.jface.text.source.IAnnotationAccess;
import org.eclipse.jface.text.source.IAnnotationAccessExtension;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Canvas;

public class VTLAnnotationAccess implements IAnnotationAccess, IAnnotationAccessExtension 
{
	@Override
	public Object getType(Annotation annotation)
	{
		return annotation.getType();
	}

	@Override
	public boolean isMultiLine(Annotation annotation)
	{
		return true;
	}

	@Override
	public boolean isTemporary(Annotation annotation)
	{
		return !annotation.isPersistent();
	}

	@Override
	public String getTypeLabel(Annotation annotation)
	{
		return annotation.getText();
	}

	@Override
	public int getLayer(Annotation annotation)
	{
		return 1;
	}

	@Override
	public void paint(Annotation annotation, GC gc, Canvas canvas, Rectangle bounds)
	{
		System.out.println("Paint");
	}

	@Override
	public boolean isPaintable(Annotation annotation)
	{
		return true;
	}

	@Override
	public boolean isSubtype(Object annotationType, Object potentialSupertype)
	{
		return false;
	}

	@Override
	public Object[] getSupertypes(Object annotationType)
	{
		return null;
	}
}
