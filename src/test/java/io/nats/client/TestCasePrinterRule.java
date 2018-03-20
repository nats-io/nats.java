// Copyright 2015-2018 The NATS Authors
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

package io.nats.client;

import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;

public class TestCasePrinterRule implements TestRule {

    private OutputStream out = null;
    private final TestCasePrinter printer = new TestCasePrinter();

    private String beforeContent = null;
    private String afterContent = null;
    private long timeStart;

    public TestCasePrinterRule(OutputStream os) {
        out = os;
    }

    private class TestCasePrinter extends ExternalResource {
        @Override
        protected void before() throws Throwable {
            timeStart = System.currentTimeMillis();
            out.write(beforeContent.getBytes());
        }

        @Override
        protected void after() {
            try {
                long timeEnd = System.currentTimeMillis();
                double seconds = (timeEnd - timeStart) / 1000.0;
                out.write((afterContent + "Time elapsed: "
                        + new DecimalFormat("0.000").format(seconds) + " sec\n").getBytes());
            } catch (IOException ioe) { /* ignore */
            }
        }
    }

    /**
     * Applies the test header/footer.
     */
    public final Statement apply(Statement statement, Description description) {
        beforeContent = "\n[TEST START] " + description.getMethodName() + "\n";
        // description.getClassName() to get class name
        afterContent = "[TEST ENDED] ";
        return printer.apply(statement, description);
    }
}
