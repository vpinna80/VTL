package it.bancaditalia.oss.vtl.eclipse.impl.markers;

import org.eclipse.jface.text.source.AnnotationPainter;
import org.eclipse.jface.text.source.IAnnotationAccess;
import org.eclipse.jface.text.source.ISourceViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Display;

public class VTLAnnotationPainter extends AnnotationPainter
{
	public VTLAnnotationPainter(ISourceViewer sourceViewer, IAnnotationAccess access)
	{
		super(sourceViewer, access);

		addAnnotationType(VTLSyntaxErrorAnnotation.ANNOT_TYPE, VTLSyntaxErrorAnnotation.ANNOT_TYPE);
		addTextStyleStrategy(VTLSyntaxErrorAnnotation.ANNOT_TYPE, new UnderlineStrategy(SWT.UNDERLINE_SQUIGGLE));
		setAnnotationTypeColor(VTLSyntaxErrorAnnotation.ANNOT_TYPE, new Color(Display.getDefault(), 255, 0, 0));
	}
}
