package it.bancaditalia.oss.vtl.eclipse.parts;

import static java.util.Collections.emptyList;
import static org.eclipse.e4.ui.workbench.modeling.EModelService.IN_MAIN_MENU;
import static org.eclipse.jface.text.ITextOperationTarget.REDO;
import static org.eclipse.jface.text.ITextOperationTarget.UNDO;
import static org.eclipse.swt.SWT.CONTROL;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.eclipse.e4.core.commands.ECommandService;
import org.eclipse.e4.ui.di.Persist;
import org.eclipse.e4.ui.model.application.MApplication;
import org.eclipse.e4.ui.model.application.ui.basic.MPart;
import org.eclipse.e4.ui.model.application.ui.menu.MHandledMenuItem;
import org.eclipse.e4.ui.workbench.modeling.EModelService;
import org.eclipse.e4.ui.workbench.modeling.EPartService;
import org.eclipse.jface.text.Document;
import org.eclipse.jface.text.TextViewerUndoManager;
import org.eclipse.jface.text.source.CompositeRuler;
import org.eclipse.jface.text.source.LineNumberRulerColumn;
import org.eclipse.jface.text.source.SourceViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;

@SuppressWarnings("restriction")
public class EditorPart
{
	public static final String EDITOR_ELEMENT_ID = "vtl-eclipse-app.editor.template";
	public static final String CLOSE_MENU_ID = "vtl-eclipse-app.menu.file.close";
	
	@Inject
	private MPart part;

	@Inject
	private EModelService modelService;

	@Inject
	private MApplication application;

	@Inject
	private EPartService partService;

	@Inject
	private ECommandService commandService;
	
	private SourceViewer editor;
	private File fileName;

	@PostConstruct
	public final void createComposite(final Composite parent)
	{
		fileName = (File) part.getTransientData().get("fileName");
		if (fileName != null)
			part.setLabel(fileName.getName());
		final CompositeRuler ruler = new CompositeRuler(5);
		final LineNumberRulerColumn lineNumberRuler = new LineNumberRulerColumn();
		ruler.addDecorator(0, lineNumberRuler);
		editor = new SourceViewer(parent, ruler, SWT.MULTI | SWT.SEARCH);
		editor.configure(new EditorConfigurator());
		MHandledMenuItem closeMenu = modelService.findElements(application, CLOSE_MENU_ID, MHandledMenuItem.class, emptyList(), IN_MAIN_MENU).get(0);
		partService.addPartListener(new PartListenerAdapter(commandService, closeMenu));
		Display.getDefault().asyncExec(this::lazyInit);
	}

	private void lazyInit()
	{
		editor.setDocument(new Document(System.lineSeparator().repeat(150)));
		editor.setDocument(load());
		editor.addTextListener(event -> part.setDirty(true));
		editor.setUndoManager(new TextViewerUndoManager(500));
		editor.getUndoManager().connect(editor);
		editor.getTextWidget().addKeyListener(new KeyAdapter() {
			@Override
			public void keyPressed(KeyEvent e)
			{
				if ((e.stateMask & CONTROL) > 0)
					if (e.keyCode == 'y' || e.keyCode == 'Y')
						editor.doOperation(REDO);
					else if (e.keyCode == 'z' || e.keyCode == 'Z')
						editor.doOperation(UNDO);
			}
		});
	}

	@Persist
	public void save()
	{
		if (fileName == null)
		{
			String selected = getSaveFile();
			if (selected != null)
				fileName = new File(selected);
		}

		if (fileName != null)
		{
			// TODO: save
			part.setDirty(false);
		}
	}

	private String getSaveFile()
	{
		FileDialog fileDialog = new FileDialog(editor.getControl().getShell(), SWT.SAVE);
		fileDialog.setText("Save as...");
		fileDialog.setFilterExtensions(new String[] { "*.vtl", "*.*" });
		fileDialog.setFilterNames(new String[] { "VTL Scripts (*.vtl)", "All files (*.*)" });
		return fileDialog.open();
	}

	private Document load()
	{
		if (fileName != null)
			try (FileReader reader = new FileReader(fileName))
			{
				StringWriter writer = new StringWriter();
				reader.transferTo(writer);
				return new Document(writer.toString());
			} 
			catch (IOException e)
			{
				e.printStackTrace();
				return new Document();
			}
		else
			return new Document();
	}
}