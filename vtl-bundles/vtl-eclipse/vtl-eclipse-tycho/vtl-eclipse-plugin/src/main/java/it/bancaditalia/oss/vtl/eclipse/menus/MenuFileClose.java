package it.bancaditalia.oss.vtl.eclipse.menus;

import static it.bancaditalia.oss.vtl.eclipse.parts.EditorPart.EDITOR_ELEMENT_ID;

import java.io.IOException;

import javax.inject.Inject;

import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.e4.ui.model.application.ui.basic.MPart;
import org.eclipse.e4.ui.workbench.modeling.EPartService;
import org.eclipse.swt.widgets.Shell;

public class MenuFileClose
{
	@Inject EPartService partService;
	
	@Execute
	public void execute(Shell shell) throws IOException
	{
		MPart part = partService.getActivePart();
		if (!EDITOR_ELEMENT_ID.equals(part.getElementId()))
			throw new IllegalStateException("Close editor but no editor focused.");
		
		if (partService.savePart(part, true))
			partService.hidePart(part, true);
	}
}
