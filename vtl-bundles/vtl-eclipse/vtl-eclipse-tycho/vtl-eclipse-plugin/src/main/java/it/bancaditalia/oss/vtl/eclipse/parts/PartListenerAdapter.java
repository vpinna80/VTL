package it.bancaditalia.oss.vtl.eclipse.parts;

import javax.inject.Singleton;

import org.eclipse.e4.core.commands.ECommandService;
import org.eclipse.e4.ui.model.application.ui.basic.MPart;
import org.eclipse.e4.ui.model.application.ui.menu.MHandledMenuItem;
import org.eclipse.e4.ui.workbench.modeling.IPartListener;

@SuppressWarnings("restriction")
@Singleton
public class PartListenerAdapter implements IPartListener
{
	private static final String CLOSE_COMMAND = "vtl-eclipse-app.command.close";
	
	private final ECommandService commandService;
	private final MHandledMenuItem closeMenu;

	public PartListenerAdapter(ECommandService commandService, MHandledMenuItem closeMenu)
	{
		this.commandService = commandService;
		this.closeMenu = closeMenu;
	}
	
	@Override
	public void partBroughtToTop(MPart part)
	{
		if (EditorPart.EDITOR_ELEMENT_ID.equals(part.getElementId()))
		{
			commandService.getCommand(CLOSE_COMMAND).setEnabled(true);
			closeMenu.setLabel("Close " + part.getLabel());
		}
	}
	
	@Override
	public void partActivated(MPart part)
	{
		if (EditorPart.EDITOR_ELEMENT_ID.equals(part.getElementId()))
		{
			commandService.getCommand(CLOSE_COMMAND).setEnabled(true);
			closeMenu.setLabel("Close " + part.getLabel());
		}
	}
	
	@Override
	public void partDeactivated(MPart part)
	{
		if (EditorPart.EDITOR_ELEMENT_ID.equals(part.getElementId()))
			commandService.getCommand(CLOSE_COMMAND).setEnabled(false);
	}
	
	@Override
	public void partHidden(MPart part)
	{

	}
	
	@Override
	public void partVisible(MPart part)
	{

	}
}
