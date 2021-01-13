package it.bancaditalia.oss.vtl.eclipse.wizards;

import java.net.MalformedURLException;
import java.util.List;

import org.eclipse.jface.layout.TreeColumnLayout;
import org.eclipse.jface.viewers.ITreeSelection;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.TreeNode;
import org.eclipse.jface.viewers.TreeNodeContentProvider;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.jface.viewers.ViewerFilter;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.wb.swt.ResourceManager;

import it.bancaditalia.oss.vtl.eclipse.impl.projects.VTLFolder;
import it.bancaditalia.oss.vtl.eclipse.impl.projects.VTLProject;
import it.bancaditalia.oss.vtl.eclipse.parts.ImageProvider;

public abstract class NewResourceSelectorPage extends WizardPage
{
	private final List<VTLProject> projects;
	private final ITreeSelection selection;
	private final String itemClass;

	private Text fileName;
	private TreeViewer treeViewer;
	
	public NewResourceSelectorPage(List<VTLProject> projectTree, ITreeSelection selection, String itemClass, String imagePackage, String imagePath)
	{
		super("New " + itemClass);
		this.selection = selection;
		this.itemClass = itemClass;
		setImageDescriptor(ResourceManager.getPluginImageDescriptor(imagePackage, imagePath));
		setTitle(itemClass);
		setMessage("Create a new " + itemClass);
		this.projects = projectTree;
	}

	@Override
	public void createControl(Composite parent)
	{
		Composite container = new Composite(parent, SWT.NONE);
		setControl(container);
		container.setLayout(new GridLayout(2, false));
		
		Label lblNewLabel = new Label(container, SWT.NONE);
		lblNewLabel.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false, 2, 1));
		lblNewLabel.setText("Select a container for the new " + itemClass);
		
		Composite composite = new Composite(container, SWT.NONE);
		GridData gd_composite = new GridData(SWT.FILL, SWT.FILL, false, false, 2, 1);
		gd_composite.heightHint = 400;
		gd_composite.minimumHeight = 400;
		composite.setLayoutData(gd_composite);
		composite.setLayout(new TreeColumnLayout());
		
		treeViewer = new TreeViewer(composite, SWT.BORDER);
		Tree tree_1 = treeViewer.getTree();
		tree_1.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent e) {
				getWizard().getContainer().updateButtons();
			}
		});
		tree_1.setHeaderVisible(false);
		tree_1.setLinesVisible(false);
		treeViewer.setLabelProvider(new LabelProvider() {
			public Image getImage(Object element)
			{
				try
				{
					return ((ImageProvider) element).getImage();
				} 
				catch (MalformedURLException e)
				{
					throw new IllegalStateException(e);
				}
			}
		});
		treeViewer.setFilters(new ViewerFilter() {
			@Override
			public boolean select(Viewer viewer, Object parentElement, Object element)
			{
				return element instanceof VTLProject || element instanceof VTLFolder;
			}
		});
		treeViewer.setContentProvider(new TreeNodeContentProvider());
		treeViewer.setInput(projects.toArray(new TreeNode[0]));
		treeViewer.setSelection(selection);
		
		Label lblNewLabel_1 = new Label(container, SWT.NONE);
		lblNewLabel_1.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false, 1, 1));
		lblNewLabel_1.setText("New " + itemClass + " name:");
		
		fileName = new Text(container, SWT.BORDER);
		fileName.addModifyListener(e -> getWizard().getContainer().updateButtons());
		fileName.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));
		
		fileName.setFocus();
	}

	public String getFileName()
	{
		return fileName.getText().trim();
	}
	
	public TreeNode getSelection()
	{
		return (TreeNode) treeViewer.getStructuredSelection().getPaths()[0].getLastSegment();
	}

	@Override
	public boolean isPageComplete()
	{
		final String text = fileName.getText();
		return !treeViewer.getStructuredSelection().isEmpty() && !text.isBlank() && text.matches("^[0-9a-zA-Z\\^\\&\\'\\@\\{\\}\\[\\]\\,\\$\\=\\!\\-\\#\\(\\)\\.\\%\\+\\~\\_ ]+$");
	}
}