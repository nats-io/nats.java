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

package io.nats.examples.chaosTestApp;

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonValue;
import io.nats.examples.chaosTestApp.support.CommandLine;

import javax.swing.*;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import java.awt.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantLock;

public class Output extends JPanel {
    public enum Screen {Left, Main, Console}

    static final ReentrantLock workLock = new ReentrantLock();
    static final ReentrantLock controlLock = new ReentrantLock();
    static final ReentrantLock debugLock = new ReentrantLock();

    static boolean console;
    static boolean work;
    static boolean debug;

    static boolean started;
    static Output workInstance;
    static Output controlInstance;
    static Output debugInstance;
    static PrintStream workLog;
    static PrintStream controlLog;
    static PrintStream debugLog;
    static String controlConsoleAreaLabel = null;

    static final int HEIGHT_REDUCTION = 45;

    static final Font DISPLAY_FONT;
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
        DISPLAY_FONT = new Font(fontName, Font.PLAIN, 14);
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        SCREEN_AVAILABLE_WIDTH = (int)screenSize.getWidth();
        SCREEN_AVAILABLE_HEIGHT = (int)screenSize.getHeight() - HEIGHT_REDUCTION;

        ROWS = SCREEN_AVAILABLE_HEIGHT / 21;
    }

    public static void start(CommandLine cmd) {
        if (started) {
            return;
        }

        started = true;
        console = cmd.uiScreen == Screen.Console;
        work = cmd.work;
        debug = cmd.debug;

        if (console && (work || debug)) {
            controlConsoleAreaLabel = "CTRL";
        }

        // LOG FILES
        if (cmd.logdir != null) {
            File f = new File(cmd.logdir);
            if (!f.exists() && !f.mkdirs()) {
                errorMessage("OUTPUT", "Unable to create logdir: " + cmd.logdir);
                System.exit(-1);
            }
            String template = "applog-which.txt";
            try {
                String fn = template.replace("which", "control");
                Path p = Paths.get(f.getAbsolutePath(), fn);
                controlLog = new PrintStream(new FileOutputStream(p.toFile()));

                if (debug) {
                    fn = template.replace("which", "debug");
                    p = Paths.get(f.getAbsolutePath(), fn);
                    debugLog = new PrintStream(new FileOutputStream(p.toFile()));
                }

                if (work) {
                    fn = template.replace("which", "work");
                    p = Paths.get(f.getAbsolutePath(), fn);
                    workLog = new PrintStream(new FileOutputStream(p.toFile()));
                }
            }
            catch (FileNotFoundException e) {
                errorMessage("OUTPUT", "Unable to create log file: " + e);
                System.exit(-1);
            }
        }

        // SCREEN
        int debugWidth = (int)(SCREEN_AVAILABLE_WIDTH * 0.42);
        int workWidth =  (int)(SCREEN_AVAILABLE_WIDTH * 0.24);
        int controlWidth = SCREEN_AVAILABLE_WIDTH - debugWidth - workWidth;
        int offset = 0;
        if (cmd.uiScreen == Screen.Left) {
            if (debug || work) {
                if (debug) {
                    debugInstance = newUi("Debug", -debugWidth, debugWidth, SCREEN_AVAILABLE_HEIGHT);
                    offset = -debugWidth;
                }
                if (work) {
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
        else if (cmd.uiScreen == Screen.Main) {
            if (debug || work) {
                if (debug) {
                    debugInstance = newUi("Debug", 0, debugWidth, SCREEN_AVAILABLE_HEIGHT);
                    offset = debugWidth;
                }
                if (work) {
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

    private static Output newUi(String name, int xLoc, int width, int height) {
        //Create and set up the window.
        JFrame frame = new JFrame(name);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        //Add contents to the window.
        Output output = new Output();
        frame.add(output);

        //Display the window.
        frame.setLocation(xLoc, 0);
        frame.setPreferredSize(new Dimension(width, height));
        frame.pack();
        frame.setVisible(true);
        return output;
    }

    private Output() {
        super(new GridLayout(1, 1));
        area = new JTextArea(ROWS, 40);
        area.setEditable(false);
        area.setFont(DISPLAY_FONT);
        add(new JScrollPane(area));
    }

    private static String time() {
        String t = "" + System.currentTimeMillis();
        return t.substring(t.length() - 9);
    }

    public static void workMessage(String label, String s) {
        if (work) {
            workLock.lock();
            try {
                if (console) {
                    consoleMessage("WORK", label, s);
                }
                else {
                    append(label, s, workInstance.area);
                }
                if (workLog != null) {
                    consoleMessage(null, label, s, workLog);
                }
            }
            finally {
                workLock.unlock();
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
        controlLock.lock();
        try {
            if (console) {
                consoleMessage(controlConsoleAreaLabel, label, s);
            }
            else {
                append(label, s, controlInstance.area);
            }
            if (workLog != null) {
                consoleMessage(null, label, s, controlLog);
            }
        }
        finally {
            controlLock.unlock();
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

    public static void debugMessage(String label, String s) {
        if (debug) {
            debugLock.lock();
            try {
                if (console) {
                    consoleMessage("DEBUG", label, s);
                }
                else {
                    debugInstance.area.append(s);
                    debugInstance.area.append("\n");
                    afterAppend(debugInstance.area);
                }
                if (debugLog != null) {
                    consoleMessage("DEBUG", label, s + "\n", controlLog);
                }
            }
            finally {
                debugLock.unlock();
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

    public static void errorMessage(String label, String s) {
        consoleMessage("ERROR", label, s, System.out);
    }

    public static void fatalMessage(String label, String s) {
        consoleMessage("FATAL", label, s, System.out);
    }

    public static void consoleMessage(String area, String label, String s) {
        consoleMessage(area, label, s, System.out);
    }

    public static void consoleMessage(String area, String label, String s, PrintStream out) {
        out.print(time());
        String llabel = label == null ? "" : " | " + label;
        out.print(area == null ? llabel : " | " + area + llabel);

        if (s.contains("\n")) {
            if (!s.startsWith("\n")) {
                out.print(" | ");
            }
            out.print(s.replace("\n", NLINDENT));
        }
        else {
            out.print(" | ");
            out.print(s);
        }
        out.println();
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
