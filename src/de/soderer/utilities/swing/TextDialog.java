package de.soderer.utilities.swing;

import java.awt.Color;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

public class TextDialog extends JDialog {
	private static final long serialVersionUID = 2020023796052296409L;

	public TextDialog(final Frame parent, String title, String text, Color backgroundcolor) {
		super(parent, title, Dialog.ModalityType.DOCUMENT_MODAL);

		final TextDialog textDialog = this;

		setResizable(false);

		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

		JTextArea textField = new JTextArea();
		textField.setText(text);
		textField.setEditable(false);
		if (backgroundcolor != null) {
			textField.setBackground(backgroundcolor);
		}
		JScrollPane textScrollpane = new JScrollPane(textField);
		textScrollpane.setPreferredSize(new Dimension(400, 200));
		
		// Text Panel
		JPanel textPanel = new JPanel();
		textPanel.setLayout(new BoxLayout(textPanel, BoxLayout.LINE_AXIS));

		textPanel.add(Box.createRigidArea(new Dimension(5, 0)));
		textPanel.add(textScrollpane);
		textPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
		JButton cancelButton = new JButton("Close");
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				textDialog.dispose();
			}
		});
		buttonPanel.add(cancelButton);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));
		panel.add(textPanel);
		panel.add(buttonPanel);

		add(panel);

		pack();

		setLocationRelativeTo(parent);
		
		getRootPane().setDefaultButton(cancelButton);
	}
}