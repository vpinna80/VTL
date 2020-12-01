package it.bancaditalia.oss.vtl.eclipse.menus;

import java.io.IOException;

import javax.inject.Inject;

import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.e4.ui.model.application.ui.basic.MPart;
import org.eclipse.e4.ui.workbench.modeling.EPartService;
import org.eclipse.swt.widgets.Shell;

import it.bancaditalia.oss.vtl.eclipse.parts.EditorPart;

public class MenuEditUndo
{
	@Inject
	private EPartService partService;
	
	@Execute
	public void execute(Shell shell) throws IOException
	{
		MPart part = partService.getActivePart();
		if (EditorPart.EDITOR_ELEMENT_ID.equals(part.getElementId()))
			((EditorPart) part.getObject()).undo();
	}
}
