package it.bancaditalia.oss.vtl.eclipse.parts;

import org.eclipse.jface.text.IDocument;
import org.eclipse.jface.text.presentation.IPresentationReconciler;
import org.eclipse.jface.text.presentation.PresentationReconciler;
import org.eclipse.jface.text.rules.DefaultDamagerRepairer;
import org.eclipse.jface.text.source.ISourceViewer;
import org.eclipse.jface.text.source.SourceViewerConfiguration;

public class EditorConfigurator extends SourceViewerConfiguration 
{
    public IPresentationReconciler getPresentationReconciler(ISourceViewer viewer)
    {
        PresentationReconciler reconciler = new PresentationReconciler();
        DefaultDamagerRepairer defDamagerRepairer = new DefaultDamagerRepairer(new EditorAntlrTokenizer());
        reconciler.setDamager(defDamagerRepairer, IDocument.DEFAULT_CONTENT_TYPE);
        reconciler.setRepairer(defDamagerRepairer, IDocument.DEFAULT_CONTENT_TYPE);
        return reconciler;
   }
}
