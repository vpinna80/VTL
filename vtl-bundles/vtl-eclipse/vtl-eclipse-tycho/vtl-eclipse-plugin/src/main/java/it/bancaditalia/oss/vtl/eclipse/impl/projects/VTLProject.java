package it.bancaditalia.oss.vtl.eclipse.impl.projects;

import java.net.MalformedURLException;

import org.eclipse.jface.viewers.TreeNode;
import org.eclipse.swt.graphics.Image;

import it.bancaditalia.oss.vtl.eclipse.parts.ImageManager;
import it.bancaditalia.oss.vtl.eclipse.parts.ImageProvider;

public class VTLProject extends TreeNode implements ImageProvider
{
	private static final String CLOSED_PROJECT_ICON_URL = "platform:/plugin/org.eclipse.ui.ide/icons/full/obj16/cprj_obj.png";
	private static final String OPENED_PROJECT_ICON_URL = "platform:/plugin/org.eclipse.ui.ide/icons/full/obj16/prj_obj.png";

	private boolean opened = true;
	
	public VTLProject(String name)
	{
		super(name);
	}
	
	@Override
	public String toString()
	{
		return getValue().toString();
	}

	@Override
	public Image getImage() throws MalformedURLException
	{
		return ImageManager.getImage(isOpened() ? OPENED_PROJECT_ICON_URL : CLOSED_PROJECT_ICON_URL);
	}

	public boolean isOpened()
	{
		return opened;
	}
}
