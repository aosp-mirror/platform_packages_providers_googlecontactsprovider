package com.android.providers.contacts;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.provider.Contacts;
import android.test.RenamingDelegatingContext;
import android.test.SyncBaseInstrumentation;
import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.OperationCanceledException;
import android.accounts.AuthenticatorException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.io.IOException;

import com.google.android.collect.Maps;
import com.google.android.googlelogin.GoogleLoginServiceConstants;

public class SyncContactsTest extends SyncBaseInstrumentation {
    private Context mTargetContext;
    private String mAccount;
    private static final String PEOPLE_PHONE_JOIN =
            "people LEFT OUTER JOIN phones ON people.primary_phone=phones._id " +
            " LEFT OUTER JOIN contact_methods ON people.primary_email=contact_methods._id" +
            " LEFT OUTER JOIN organizations ON people.primary_organization=organizations._id" +
            " ORDER BY people._sync_id;";

    private static final Set<String> PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP = new HashSet<String>();

    static {
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People._ID);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People.PRIMARY_PHONE_ID);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People._SYNC_MARK);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People._SYNC_TIME);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People._SYNC_LOCAL_ID);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People._SYNC_VERSION);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People._SYNC_DIRTY);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People.TIMES_CONTACTED);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People.LAST_TIME_CONTACTED);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.Phones._ID);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.Phones.PERSON_ID);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People.PRIMARY_EMAIL_ID);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People.PRIMARY_ORGANIZATION_ID);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.People.ContactMethods._ID);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.Organizations.PERSON_ID);
        PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP.add(Contacts.Organizations._ID);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mTargetContext = getInstrumentation().getTargetContext();
        mAccount = getAccount();
    }

    /**
     * A Simple test that syncs the contacts provider.
     * @throws Exception
     */
    public void testSync() throws Exception {
        cancelSyncsandDisableAutoSync();
        syncProvider(Contacts.CONTENT_URI, mAccount, Contacts.AUTHORITY);
    }

    private String getAccount() {
        try {
            Account[] accounts = AccountManager.get(mTargetContext)
                    .blockingGetAccountsWithTypeAndFeatures(
                            GoogleLoginServiceConstants.ACCOUNT_TYPE,
                            new String[]{GoogleLoginServiceConstants.FEATURE_GOOGLE_OR_DASHER});
            if (accounts.length > 0) {
                return accounts[0].mName;
            }
        } catch (OperationCanceledException e) {
            // handle below
        } catch (IOException e) {
            // handle below
        } catch (AuthenticatorException e) {
            // handle below
        }
        return null;
    }

    /**
     * This test compares the two contacts databases.
     * This works well with the puppetmaster automated script.
     * @throws Exception
     */
    public void testCompareResults() throws Exception {
        ContactsProvider incrementalContentProvider =
                RenamingDelegatingContext.providerWithRenamedContext(ContactsProvider.class,
                        mTargetContext, "", true);

        ContactsProvider initialSyncContentProvider =
                RenamingDelegatingContext.providerWithRenamedContext(ContactsProvider.class,
                        mTargetContext, "initialsync.", true);

        SQLiteDatabase incrementalDb = incrementalContentProvider.getDatabase();
        SQLiteDatabase initialSyncDb = initialSyncContentProvider.getDatabase();

        Cursor incrementalPeopleCursor = incrementalDb.rawQuery("select * from " +
                                         PEOPLE_PHONE_JOIN, null);
        Cursor initialPeopleCursor = initialSyncDb.rawQuery("select * from " +
                                     PEOPLE_PHONE_JOIN, null);

        assertNotSame("Incremental db has no values - check test configuration",
                      incrementalPeopleCursor.getCount(), 0);
        try {
            compareCursors(incrementalPeopleCursor, initialPeopleCursor,
                           PEOPLE_PHONES_JOIN_COLUMNS_TO_SKIP, "People");
        } finally {
            incrementalPeopleCursor.close();
            initialPeopleCursor.close();
        }
    }

    private void compareCursors(Cursor incrementalCursor,
                                Cursor initialSyncCursor, Set<String> columnsToSkip,
                                String tableName) {

        assertEquals(tableName + " count failed to match", incrementalCursor.getCount(),
                             initialSyncCursor.getCount());

        String[] cols = incrementalCursor.getColumnNames();
        int length = cols.length;
        Map<String,String> row = Maps.newHashMap();
        
        while (incrementalCursor.moveToNext() && initialSyncCursor.moveToNext()) {
            for (int i = 0; i < length; i++) {
                String col = cols[i];
                if (columnsToSkip != null && columnsToSkip.contains(col)) {
                    continue;
                }
                row.put(col, incrementalCursor.getString(i));
                assertEquals("Row: " + row + " .Column: " + cols[i] + " failed to match",
                             incrementalCursor.getString(i), initialSyncCursor.getString(i));
            }
        }
    }
}
