package com.android.providers.contactstests;

import com.android.providers.contacts.SyncContactsTest;

import junit.framework.TestSuite;

import android.test.InstrumentationTestRunner;
import android.test.InstrumentationTestSuite;
import android.test.suitebuilder.annotation.Suppress;

@Suppress
public class SyncContactsInstrumentation extends InstrumentationTestRunner {
    public TestSuite getAllTests() {
        TestSuite suite = new InstrumentationTestSuite(this);
        suite.addTestSuite(SyncContactsTest.class);
        return suite;
    }

    public ClassLoader getLoader() {
        return SyncContactsTest.class.getClassLoader();
    }
}
