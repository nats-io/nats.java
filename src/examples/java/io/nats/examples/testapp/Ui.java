// Copyright 2023 The NATS Authors
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
import javax.swing.text.Document;
import java.awt.*;

public class Ui extends JPanel {
    public enum Screen {Left, Main}

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

    public static void start(Screen screen, boolean showWork, boolean showDebug) {
        if (controlInstance != null) {
            return;
        }

        int debugWidth = (int)(SCREEN_AVAILABLE_WIDTH * 0.42);
        int workWidth =  (int)(SCREEN_AVAILABLE_WIDTH * 0.24);
        int controlWidth = SCREEN_AVAILABLE_WIDTH - debugWidth - workWidth;
        int offset = 0;
        if (screen == Screen.Left) {
            if (showDebug || showWork) {
                if (showDebug) {
                    debugInstance = newUi("Debug", -debugWidth, debugWidth, SCREEN_AVAILABLE_HEIGHT);
                    offset = -debugWidth;
                }
                if (showWork) {
                    offset -= controlWidth;
                    controlInstance = newUi("Control", offset, controlWidth, SCREEN_AVAILABLE_HEIGHT);
                    workInstance = newUi("Work", offset - workWidth, workWidth, SCREEN_AVAILABLE_HEIGHT);
                }
                else {
                    offset -= debugWidth;
                    controlInstance = newUi("Control", offset, debugWidth, SCREEN_AVAILABLE_HEIGHT);
                }
            }
            else {
                offset = -SCREEN_AVAILABLE_WIDTH / 2;
                controlInstance = newUi("Control", offset, -offset, SCREEN_AVAILABLE_HEIGHT);
            }
        }
        else {
            if (showDebug || showWork) {
                if (showDebug) {
                    debugInstance = newUi("Debug", 0, debugWidth, SCREEN_AVAILABLE_HEIGHT);
                    offset = debugWidth;
                }
                if (showWork) {
                    controlInstance = newUi("Control", offset, controlWidth, SCREEN_AVAILABLE_HEIGHT);
                    offset += controlWidth;
                    workInstance = newUi("Work", offset, workWidth, SCREEN_AVAILABLE_HEIGHT);
                }
                else {
                    controlInstance = newUi("Control", offset, debugWidth, SCREEN_AVAILABLE_HEIGHT);
                }
            }
            else {
                offset = SCREEN_AVAILABLE_WIDTH / 2;
                controlInstance = newUi("Control", 0, offset, SCREEN_AVAILABLE_HEIGHT);
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

    public static void workMessage(String label, String s) {
        if (workInstance != null) {
            synchronized (workLock) {
                append(label, s, workInstance.area);
            }
        }
    }

    public static void controlMessage(String label, JsonSerializable j) {
        controlMessage(label, formatted(j));
    }

    public static void controlMessage(String label, String jvLabel, JsonValue jv) {
        controlMessage(label, formatted(jv).replace("JsonValue", jvLabel));
    }

    public static void controlMessage(String label, String s) {
        synchronized (controlLock) {
            append(label, s, controlInstance.area);
        }
    }

    public static void dumpControl() {
        dump("Control", controlInstance.area.getDocument());
    }

    private static void dump(String label, Document document) {
        try {
            System.out.println("----------------------------------------------------------------------------------------------------");
            System.out.println("UI-" + label);
            System.out.println("----------------------------------------------------------------------------------------------------");
            System.out.println(document.getText(0, document.getLength()));
            System.out.println("----------------------------------------------------------------------------------------------------");
        }
        catch (BadLocationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
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

    static final String NLINDENT = "\n    ";
    private static void append(String label, String s, JTextArea area) {
        if (s.contains("\n")) {
            String timeLabel = time() + " | " + label;
            area.append(timeLabel);
            if (!s.startsWith("\n")) {
                area.append(" | ");
            }
            area.append(s.replace("\n", NLINDENT));
        }
        else {
            area.append(time());
            area.append(" | ");
            area.append(label);
            area.append(" | ");
            area.append(s);
        }
        area.append("\n");
        afterAppend(area);
    }

    public static void consoleMessage(String label, String s) {
        if (s.contains("\n")) {
            String timeLabel = time() + " | " + label;
            System.out.print(timeLabel);
            if (!s.startsWith("\n")) {
                System.out.print(" | ");
            }
            System.out.print(s.replace("\n", NLINDENT));
        }
        else {
            System.out.print(time());
            System.out.print(" | ");
            System.out.print(label);
            System.out.print(" | ");
            System.out.print(s);
        }
        System.out.println();
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

    public static String FN = "\n  ";
    public static String FBN = "{\n  ";
    public static String formatted(JsonSerializable j) {
        return j.getClass().getSimpleName() + j.toJson()
            .replace("{\"", FBN + "\"").replace(",", "," + FN);
    }

    public static String formatted(Object o) {
        return formatted(o.toString());
    }

    public static String formatted(String s) {
        return s.replace("{", FBN).replace(", ", "," + FN);
    }
}
