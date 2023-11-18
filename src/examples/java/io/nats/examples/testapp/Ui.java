// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.examples.testapp;

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonValue;

import javax.swing.*;
import javax.swing.text.BadLocationException;
import java.awt.*;

public class Ui extends JPanel {
    public enum Monitor {Left, Center}

    static Ui workInstance;
    static Ui controlInstance;
    static Ui debugInstance;
    static final Object workLock = new Object();
    static final Object controlLock = new Object();
    static final Object debugLock = new Object();

    static final int HEIGHT_REDUCTION = 45;

    static final Font UI_FONT;
    static final int SCREEN_AVAILABLE_WIDTH;
    static final int SCREEN_AVAILABLE_HEIGHT;
    static final int ROWS;

    JTextArea area;

    static {
        // figure UI_FONT
        String fontName = Font.MONOSPACED;
        GraphicsEnvironment localEnv;
        localEnv= GraphicsEnvironment.getLocalGraphicsEnvironment();
        String allfonts[] = localEnv.getAvailableFontFamilyNames();
        for (String allfont : allfonts) {
            if (allfont.equals("JetBrains Mono")) {
                fontName = allfont;
                break;
            }
        }
        UI_FONT = new Font(fontName, Font.PLAIN, 14);

        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        SCREEN_AVAILABLE_WIDTH = (int)screenSize.getWidth();
        SCREEN_AVAILABLE_HEIGHT = (int)screenSize.getHeight() - HEIGHT_REDUCTION;

        ROWS = SCREEN_AVAILABLE_HEIGHT / 21;
    }

    public static void start(Monitor monitor, boolean showWork, boolean showDebug) {
        if (controlInstance != null) {
            return;
        }

        int debugWidth = (int)(SCREEN_AVAILABLE_WIDTH * 0.42);
        int workWidth =  (int)(SCREEN_AVAILABLE_WIDTH * 0.24);
        int controlWidth = SCREEN_AVAILABLE_WIDTH - debugWidth - workWidth;
        int offset = 0;
        if (monitor == Monitor.Left) {
            if (showDebug) {
                debugInstance = newUi("Debug", -debugWidth, debugWidth, SCREEN_AVAILABLE_HEIGHT);
                offset = -debugWidth;
            }
            offset -= controlWidth;
            controlInstance = newUi("Control", offset, controlWidth, SCREEN_AVAILABLE_HEIGHT);
            if (showWork) {
                workInstance = newUi("Work", offset - workWidth, workWidth, SCREEN_AVAILABLE_HEIGHT);
            }
        }
        else {
            if (showDebug) {
                debugInstance = newUi("Debug", 0, debugWidth, SCREEN_AVAILABLE_HEIGHT);
                offset = debugWidth;
            }
            controlInstance = newUi("Control", offset, controlWidth, SCREEN_AVAILABLE_HEIGHT);
            offset += controlWidth;
            if (showWork) {
                workInstance = newUi("Work", offset, workWidth, SCREEN_AVAILABLE_HEIGHT);
            }
        }
    }

    private static Ui newUi(String name, int xLoc, int width, int height) {
        //Create and set up the window.
        JFrame frame = new JFrame(name);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        //Add contents to the window.
        Ui ui = new Ui();
        frame.add(ui);

        //Display the window.
        frame.setLocation(xLoc, 0);
        frame.setPreferredSize(new Dimension(width, height));
        frame.pack();
        frame.setVisible(true);
        return ui;
    }

    private Ui() {
        super(new GridLayout(1, 1));
        area = new JTextArea(ROWS, 40);
        area.setEditable(false);
        area.setFont(UI_FONT);
        add(new JScrollPane(area));
    }

    private static String time() {
        String t = "" + System.currentTimeMillis();
        return t.substring(t.length() - 9);
    }

    public static void workMessage(String component, String s) {
        if (workInstance != null) {
            synchronized (workLock) {
                append(component, s, workInstance.area);
            }
        }
    }

    public static void controlMessage(String component, JsonSerializable j) {
        controlMessage(component, formatted(j));
    }

    public static void controlMessage(String component, String label, JsonValue jv) {
        controlMessage(component, formatted(jv).replace("JsonValue", label));
    }

    public static void controlMessage(String component, String s) {
        synchronized (controlLock) {
            append(component, s, controlInstance.area);
        }
    }

    public static void debugMessage(String s) {
        if (debugInstance != null) {
            synchronized (debugLock) {
                debugInstance.area.append(s);
                debugInstance.area.append("\n");
                afterAppend(debugInstance.area);
            }
        }
    }

    private static void append(String component, String s, JTextArea area) {
        area.append(time());
        area.append(" | ");
        area.append(component);
        area.append(" | ");
        area.append(s);
        area.append("\n");
        afterAppend(area);
    }

    private static void afterAppend(JTextArea area) {
        int c = area.getLineCount();
        while (c > ROWS) {
            try {
                int end = area.getLineEndOffset(1);
                area.getDocument().remove(0, end);
                c = area.getLineCount();
            }
            catch (BadLocationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static String FORMAT_INDENT_SPACES = "      ";
    public static String FNI = "\n" + FORMAT_INDENT_SPACES;
    public static String FBNI = "{\n" + FORMAT_INDENT_SPACES;
    public static String formatted(JsonSerializable j) {
        return j.getClass().getSimpleName() + j.toJson().replace("{\"", FBNI + "\"").replace(",", ",\n    ");
    }

    public static String formatted(Object o) {
        return formatted(o.toString());
    }

    public static String formatted(String s) {
        return s.replace("{", FBNI).replace(", ", "," + FNI);
    }
}
