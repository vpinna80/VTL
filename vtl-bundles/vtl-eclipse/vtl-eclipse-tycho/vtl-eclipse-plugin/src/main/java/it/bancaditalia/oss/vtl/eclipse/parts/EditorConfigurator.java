package it.bancaditalia.oss.vtl.eclipse.parts;

import org.eclipse.jface.text.IDocument;
import org.eclipse.jface.text.presentation.IPresentationReconciler;
import org.eclipse.jface.text.presentation.PresentationReconciler;
import org.eclipse.jface.text.source.ISourceViewer;
import org.eclipse.jface.text.source.SourceViewerConfiguration;

import it.bancaditalia.oss.vtl.eclipse.impl.markers.VTLDocumentRepairer;

public class EditorConfigurator extends SourceViewerConfiguration
{
	private PresentationReconciler reconciler;

	public EditorConfigurator()
	{
		reconciler = new PresentationReconciler();
		
		VTLDocumentRepairer defDamagerRepairer = new VTLDocumentRepairer();
		reconciler.setDamager(defDamagerRepairer, IDocument.DEFAULT_CONTENT_TYPE);
		reconciler.setRepairer(defDamagerRepairer, IDocument.DEFAULT_CONTENT_TYPE);	
	}
	
	public IPresentationReconciler getPresentationReconciler(ISourceViewer viewer)
	{
		return reconciler;
	}
}
