/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package com.android.providers.contacts;

import com.google.android.collect.Sets;
import com.google.android.providers.AbstractGDataSyncAdapter;

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.SyncAdapter;
import android.content.SyncContext;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.os.SystemClock;
import android.provider.Contacts;
import android.text.TextUtils;
import android.accounts.Account;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A subclass of the platform contacts provider that adds the Google contacts
 * sync adapter.
 */
public class GoogleContactsProvider extends ContactsProvider {
    private static final int PURGE_CONTACTS_DELAY_IN_MS = 30000;
    private static final String ACTION_PURGE_CONTACTS =
            "com.android.providers.contacts.PURGE_CONTACTS";

    /**
     * SQL query that deletes all contacts for a given account that are not a member of
     * at least one group that has the "should_sync" column set to a non-zero value.
     */
    private static final String PURGE_UNSYNCED_CONTACTS_SQL = ""
            + "DELETE FROM people "
            + "WHERE (_id IN ("
            + "   SELECT person "
            + "   FROM ("
            + "     SELECT MAX(should_sync) AS max_should_sync, person "
            + "     FROM ("
            + "       SELECT should_sync, person "
            + "       FROM groupmembership AS gm "
            + "         OUTER JOIN groups AS g "
            + "         ON (gm.group_id=g._id "
            + "           OR (gm.group_sync_id=g._sync_id "
            + "               AND gm.group_sync_account=g._sync_account "
            + "               AND gm.group_sync_account_type=g._sync_account_type))) "
            + "       GROUP BY person) "
            + "   WHERE max_should_sync=0)"
            + "  OR _id NOT IN (SELECT person FROM groupmembership))"
            + " AND _sync_dirty=0 "
            + " AND _sync_account=? "
            + " AND _sync_account_type=?";

    private SyncAdapter mSyncAdapter = null;
    private AlarmManager mAlarmService = null;

    @Override
    public boolean onCreate() {
        BroadcastReceiver receiver = new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                if (ACTION_PURGE_CONTACTS.equals(intent.getAction())) {
                    purgeContacts((Account) intent.getParcelableExtra("account"));
                }
            }
        };
        getContext().registerReceiver(receiver, new IntentFilter(ACTION_PURGE_CONTACTS));
        return super.onCreate();
    }

    @Override
    public synchronized SyncAdapter getSyncAdapter() {
        if (mSyncAdapter != null) {
            return mSyncAdapter;
        }
        
        mSyncAdapter = new ContactsSyncAdapter(getContext(), this);
        return mSyncAdapter;
    }

    @Override
    protected void onLocalChangesForAccount(final ContentResolver resolver, Account account,
            boolean groupsModified) {
        ContactsSyncAdapter.updateSubscribedFeeds(resolver, account);
        if (groupsModified) {
            schedulePurge(account);
        }
    }

    /**
     * Delete any non-sync_dirty contacts associated with the given account
     * that are not in any of the synced groups.
     */
    private void schedulePurge(Account account) {
        if (isTemporary()) {
            throw new IllegalStateException("this must not be called on temp providers");
        }
        ensureAlarmService();
        final Intent intent = new Intent(ACTION_PURGE_CONTACTS);
        intent.putExtra("account", account);
        final PendingIntent pendingIntent = PendingIntent.getBroadcast(
                getContext(), 0 /* ignored */, intent, 0);
        mAlarmService.set(AlarmManager.ELAPSED_REALTIME_WAKEUP,
                SystemClock.elapsedRealtime() + PURGE_CONTACTS_DELAY_IN_MS,
                pendingIntent);
    }

    private void ensureAlarmService() {
        if (mAlarmService == null) {
            mAlarmService = (AlarmManager) getContext().getSystemService(Context.ALARM_SERVICE);
        }
    }

    @Override
    public void onSyncStop(SyncContext context, boolean success) {
        super.onSyncStop(context, success);
        purgeContacts(getSyncingAccount());
    }

    private void purgeContacts(Account account) {
        if (isTemporary()) {
            throw new IllegalStateException("this must not be called on temp providers");
        }
        SQLiteDatabase db = getDatabase();
        db.beginTransaction();
        try {
            // TODO(fredq) should be using account instead of null
            final String value = Contacts.Settings.getSetting(getContext().getContentResolver(),
                    null, Contacts.Settings.SYNC_EVERYTHING);
            final boolean shouldSyncEverything = !TextUtils.isEmpty(value) && !"0".equals(value);
            if (!shouldSyncEverything) {
                db.execSQL(PURGE_UNSYNCED_CONTACTS_SQL, new String[]{account.mName, account.mType});
            }

            // remove any feeds in the SyncData that aren't in the current sync set.
            Set<String> feedsToSync = Sets.newHashSet();
            feedsToSync.add(ContactsSyncAdapter.getGroupsFeedForAccount(account));
            ContactsSyncAdapter.addContactsFeedsToSync(getContext().getContentResolver(), account,
                    feedsToSync);
            AbstractGDataSyncAdapter.GDataSyncData syncData = readSyncData(account);
            if (syncData != null) {
                Iterator<Map.Entry<String, AbstractGDataSyncAdapter.GDataSyncData.FeedData>> iter =
                        syncData.feedData.entrySet().iterator();
                boolean updatedSyncData = false;
                while (iter.hasNext()) {
                    Map.Entry<String, AbstractGDataSyncAdapter.GDataSyncData.FeedData> entry =
                            iter.next();
                    if (!feedsToSync.contains(entry.getKey())) {
                        iter.remove();
                        updatedSyncData = true;
                    }
                }
                if (updatedSyncData) {
                    writeSyncData(account, syncData);
                }
            }
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
    }

    private AbstractGDataSyncAdapter.GDataSyncData readSyncData(Account account) {
        if (!getDatabase().inTransaction()) {
            throw new IllegalStateException("you can only call this from within a transaction");
        }
        Cursor c = getDatabase().query("_sync_state", new String[]{"data"},
                "_sync_account=? AND _sync_account_type=?",
                new String[]{account.mName, account.mType}, null, null, null);
        try {
            byte[] data = null;
            if (c.moveToNext()) data = c.getBlob(0);
            return ContactsSyncAdapter.newGDataSyncDataFromBytes(data);
        } finally {
            c.close();
        }
    }

    private void writeSyncData(Account account, AbstractGDataSyncAdapter.GDataSyncData syncData) {
        final SQLiteDatabase db = getDatabase();
        if (!db.inTransaction()) {
            throw new IllegalStateException("you can only call this from within a transaction");
        }
        db.delete("_sync_state", "_sync_account=? AND _sync_account_type=?",
                new String[]{account.mName, account.mType});
        ContentValues values = new ContentValues();
        values.put("data", ContactsSyncAdapter.newBytesFromGDataSyncData(syncData));
        values.put("_sync_account", account.mName);
        values.put("_sync_account_type", account.mType);
        db.insert("_sync_state", "_sync_account", values);
    }
}

