package it.bancaditalia.oss.vtl.eclipse.menus;

import static org.eclipse.e4.ui.workbench.modeling.EPartService.PartState.ACTIVATE;

import javax.inject.Inject;

import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.e4.ui.model.application.ui.basic.MPart;
import org.eclipse.e4.ui.workbench.modeling.EPartService;
import org.eclipse.swt.widgets.Shell;

public class MenuFileNew
{
	@Inject EPartService partService;
	
	@Execute
	public void execute(Shell shell)
	{
		MPart editorPart = partService.createPart("vtl-eclipse-app.editor.template");
		partService.showPart(editorPart, ACTIVATE);
	}
}
