/*
** Copyright 2007, The Android Open Source Project
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** See the License for the specific language governing permissions and
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** limitations under the License.
*/

package com.android.providers.contacts;

import com.google.android.collect.Sets;
import com.google.android.gdata.client.AndroidGDataClient;
import com.google.android.gdata.client.AndroidXmlParserFactory;
import com.google.android.googlelogin.GoogleLoginServiceBlockingHelper;
import com.google.android.googlelogin.GoogleLoginServiceNotFoundException;
import com.google.android.providers.AbstractGDataSyncAdapter;
import com.google.wireless.gdata.client.GDataServiceClient;
import com.google.wireless.gdata.client.QueryParams;
import com.google.wireless.gdata.client.HttpException;
import com.google.wireless.gdata.contacts.client.ContactsClient;
import com.google.wireless.gdata.contacts.data.ContactEntry;
import com.google.wireless.gdata.contacts.data.ContactsElement;
import com.google.wireless.gdata.contacts.data.EmailAddress;
import com.google.wireless.gdata.contacts.data.GroupEntry;
import com.google.wireless.gdata.contacts.data.GroupMembershipInfo;
import com.google.wireless.gdata.contacts.data.ImAddress;
import com.google.wireless.gdata.contacts.data.Organization;
import com.google.wireless.gdata.contacts.data.PhoneNumber;
import com.google.wireless.gdata.contacts.data.PostalAddress;
import com.google.wireless.gdata.contacts.parser.xml.XmlContactsGDataParserFactory;
import com.google.wireless.gdata.data.Entry;
import com.google.wireless.gdata.data.ExtendedProperty;
import com.google.wireless.gdata.data.Feed;
import com.google.wireless.gdata.data.MediaEntry;
import com.google.wireless.gdata.parser.ParseException;

import org.json.JSONException;
import org.json.JSONObject;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.SyncContext;
import android.content.SyncResult;
import android.content.SyncableContentProvider;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.net.Uri;
import android.os.Bundle;
import android.os.SystemProperties;
import android.provider.Contacts;
import android.provider.Contacts.ContactMethods;
import android.provider.Contacts.Extensions;
import android.provider.Contacts.GroupMembership;
import android.provider.Contacts.Groups;
import android.provider.Contacts.Organizations;
import android.provider.Contacts.People;
import android.provider.Contacts.Phones;
import android.provider.Contacts.Photos;
import android.provider.SubscribedFeeds;
import android.provider.SyncConstValue;
import android.text.TextUtils;
import android.util.Config;
import android.util.Log;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Implements a SyncAdapter for Contacts
 */
public class ContactsSyncAdapter extends AbstractGDataSyncAdapter {
    private static String CONTACTS_FEED_URL = "http://www.google.com/m8/feeds/contacts/";
    private static String GROUPS_FEED_URL = "http://www.google.com/m8/feeds/groups/";
    private static String PHOTO_FEED_URL = "http://www.google.com/m8/feeds/photos/media/";

    private final ContactsClient mContactsClient;

    private static final String[] sSubscriptionProjection =
            new String[] {
                    SubscribedFeeds.Feeds._SYNC_ACCOUNT,
                    SubscribedFeeds.Feeds.FEED,
                    SubscribedFeeds.Feeds._ID};

    private static final HashMap<Byte, Integer> ENTRY_TYPE_TO_PROVIDER_PHONE;
    private static final HashMap<Byte, Integer> ENTRY_TYPE_TO_PROVIDER_EMAIL;
    private static final HashMap<Byte, Integer> ENTRY_TYPE_TO_PROVIDER_IM;
    private static final HashMap<Byte, Integer> ENTRY_TYPE_TO_PROVIDER_POSTAL;
    private static final HashMap<Byte, Integer> ENTRY_TYPE_TO_PROVIDER_ORGANIZATION;
    private static final HashMap<Integer, Byte> PROVIDER_TYPE_TO_ENTRY_PHONE;
    private static final HashMap<Integer, Byte> PROVIDER_TYPE_TO_ENTRY_EMAIL;
    private static final HashMap<Integer, Byte> PROVIDER_TYPE_TO_ENTRY_IM;
    private static final HashMap<Integer, Byte> PROVIDER_TYPE_TO_ENTRY_POSTAL;
    private static final HashMap<Integer, Byte> PROVIDER_TYPE_TO_ENTRY_ORGANIZATION;

    private static final HashMap<Byte, Integer> ENTRY_IM_PROTOCOL_TO_PROVIDER_PROTOCOL;
    private static final HashMap<Integer, Byte> PROVIDER_IM_PROTOCOL_TO_ENTRY_PROTOCOL;

    private static final int MAX_MEDIA_ENTRIES_PER_SYNC = 10;

    // Only valid during a sync operation.
    // If set then a getServerDiffs() was performed during this sync.
    private boolean mPerformedGetServerDiffs;

    // Only valid during a sync. If set then this sync was a forced sync request
    private boolean mSyncForced;

    private int mPhotoDownloads;
    private int mPhotoUploads;

    private static final String IMAGE_MIME_TYPE = "image/*";

    static {
        HashMap<Byte, Integer> map;

        map = new HashMap<Byte, Integer>();
        map.put(ImAddress.PROTOCOL_AIM, ContactMethods.PROTOCOL_AIM);
        map.put(ImAddress.PROTOCOL_GOOGLE_TALK, ContactMethods.PROTOCOL_GOOGLE_TALK);
        map.put(ImAddress.PROTOCOL_ICQ, ContactMethods.PROTOCOL_ICQ);
        map.put(ImAddress.PROTOCOL_JABBER, ContactMethods.PROTOCOL_JABBER);
        map.put(ImAddress.PROTOCOL_MSN, ContactMethods.PROTOCOL_MSN);
        map.put(ImAddress.PROTOCOL_QQ, ContactMethods.PROTOCOL_QQ);
        map.put(ImAddress.PROTOCOL_SKYPE, ContactMethods.PROTOCOL_SKYPE);
        map.put(ImAddress.PROTOCOL_YAHOO, ContactMethods.PROTOCOL_YAHOO);
        ENTRY_IM_PROTOCOL_TO_PROVIDER_PROTOCOL = map;
        PROVIDER_IM_PROTOCOL_TO_ENTRY_PROTOCOL = swapMap(map);

        map = new HashMap<Byte, Integer>();
        map.put(EmailAddress.TYPE_HOME, ContactMethods.TYPE_HOME);
        map.put(EmailAddress.TYPE_WORK, ContactMethods.TYPE_WORK);
        map.put(EmailAddress.TYPE_OTHER, ContactMethods.TYPE_OTHER);
        map.put(EmailAddress.TYPE_NONE, ContactMethods.TYPE_CUSTOM);
        ENTRY_TYPE_TO_PROVIDER_EMAIL = map;
        PROVIDER_TYPE_TO_ENTRY_EMAIL = swapMap(map);

        map = new HashMap<Byte, Integer>();
        map.put(PhoneNumber.TYPE_HOME, Phones.TYPE_HOME);
        map.put(PhoneNumber.TYPE_MOBILE, Phones.TYPE_MOBILE);
        map.put(PhoneNumber.TYPE_PAGER, Phones.TYPE_PAGER);
        map.put(PhoneNumber.TYPE_WORK, Phones.TYPE_WORK);
        map.put(PhoneNumber.TYPE_HOME_FAX, Phones.TYPE_FAX_HOME);
        map.put(PhoneNumber.TYPE_WORK_FAX, Phones.TYPE_FAX_WORK);
        map.put(PhoneNumber.TYPE_OTHER, Phones.TYPE_OTHER);
        map.put(PhoneNumber.TYPE_NONE, Phones.TYPE_CUSTOM);
        ENTRY_TYPE_TO_PROVIDER_PHONE = map;
        PROVIDER_TYPE_TO_ENTRY_PHONE = swapMap(map);

        map = new HashMap<Byte, Integer>();
        map.put(PostalAddress.TYPE_HOME, ContactMethods.TYPE_HOME);
        map.put(PostalAddress.TYPE_WORK, ContactMethods.TYPE_WORK);
        map.put(PostalAddress.TYPE_OTHER, ContactMethods.TYPE_OTHER);
        map.put(PostalAddress.TYPE_NONE, ContactMethods.TYPE_CUSTOM);
        ENTRY_TYPE_TO_PROVIDER_POSTAL = map;
        PROVIDER_TYPE_TO_ENTRY_POSTAL = swapMap(map);

        map = new HashMap<Byte, Integer>();
        map.put(ImAddress.TYPE_HOME, ContactMethods.TYPE_HOME);
        map.put(ImAddress.TYPE_WORK, ContactMethods.TYPE_WORK);
        map.put(ImAddress.TYPE_OTHER, ContactMethods.TYPE_OTHER);
        map.put(ImAddress.TYPE_NONE, ContactMethods.TYPE_CUSTOM);
        ENTRY_TYPE_TO_PROVIDER_IM = map;
        PROVIDER_TYPE_TO_ENTRY_IM = swapMap(map);

        map = new HashMap<Byte, Integer>();
        map.put(Organization.TYPE_WORK, Organizations.TYPE_WORK);
        map.put(Organization.TYPE_OTHER, Organizations.TYPE_OTHER);
        map.put(Organization.TYPE_NONE, Organizations.TYPE_CUSTOM);
        ENTRY_TYPE_TO_PROVIDER_ORGANIZATION = map;
        PROVIDER_TYPE_TO_ENTRY_ORGANIZATION = swapMap(map);
    }

    private static <A, B> HashMap<B, A> swapMap(HashMap<A, B> originalMap) {
        HashMap<B, A> newMap = new HashMap<B,A>();
        for (Map.Entry<A, B> entry : originalMap.entrySet()) {
            final B originalValue = entry.getValue();
            if (newMap.containsKey(originalValue)) {
                throw new IllegalArgumentException("value " + originalValue
                        + " was already encountered");
            }
            newMap.put(originalValue, entry.getKey());
        }
        return newMap;
    }

    protected ContactsSyncAdapter(Context context, SyncableContentProvider provider) {
        super(context, provider);
        mContactsClient = new ContactsClient(
                new AndroidGDataClient(context),
                new XmlContactsGDataParserFactory(new AndroidXmlParserFactory()));
    }

    protected GDataServiceClient getGDataServiceClient() {
        return mContactsClient;
    }

    @Override
    protected Entry newEntry() {
        throw new UnsupportedOperationException("this should never be used");
    }

    protected String getFeedUrl(String account) {
        throw new UnsupportedOperationException("this should never be used");
    }

    protected Class getFeedEntryClass() {
        throw new UnsupportedOperationException("this should never be used");
    }

    protected Class getFeedEntryClass(String feed) {
        if (feed.startsWith(rewriteUrlforAccount(getAccount(), GROUPS_FEED_URL))) {
            return GroupEntry.class;
        }
        if (feed.startsWith(rewriteUrlforAccount(getAccount(), CONTACTS_FEED_URL))) {
            return ContactEntry.class;
        }
        return null;
    }

    @Override
    public void getServerDiffs(SyncContext context, SyncData baseSyncData,
            SyncableContentProvider tempProvider,
            Bundle extras, Object syncInfo, SyncResult syncResult) {
        mPerformedGetServerDiffs = true;
        GDataSyncData syncData = (GDataSyncData)baseSyncData;

        ArrayList<String> feedsToSync = new ArrayList<String>();

        if (extras != null && extras.containsKey("feed")) {
            feedsToSync.add((String) extras.get("feed"));
        } else {
            feedsToSync.add(getGroupsFeedForAccount(getAccount()));
            addContactsFeedsToSync(getContext().getContentResolver(), getAccount(), feedsToSync);
            feedsToSync.add(getPhotosFeedForAccount(getAccount()));
        }

        for (String feed : feedsToSync) {
            context.setStatusText("Downloading\u2026");
            if (getPhotosFeedForAccount(getAccount()).equals(feed)) {
                getServerPhotos(context, feed, MAX_MEDIA_ENTRIES_PER_SYNC, syncData, syncResult);
            } else {
                final Class feedEntryClass = getFeedEntryClass(feed);
                if (feedEntryClass != null) {
                    getServerDiffsImpl(context, tempProvider, feedEntryClass,
                            feed, null, getMaxEntriesPerSync(), syncData, syncResult);
                } else {
                    if (Config.LOGD) {
                        Log.d(TAG, "ignoring sync request for unknown feed " + feed);
                    }
                }
            }
            if (syncResult.hasError()) {
                break;
            }
        }
    }

    /**
     * Look at the groups sync settings and the overall sync preference to determine which
     * feeds to sync and add them to the feedsToSync list.
     */
    public static void addContactsFeedsToSync(ContentResolver cr, String account,
            Collection<String> feedsToSync) {
        boolean shouldSyncEverything = getShouldSyncEverything(cr, account);
        if (shouldSyncEverything) {
            feedsToSync.add(getContactsFeedForAccount(account));
            return;
        }

        Cursor cursor = cr.query(Contacts.Groups.CONTENT_URI, new String[]{Groups._SYNC_ID},
                "_sync_account=? AND should_sync>0", new String[]{account}, null);
        try {
            while (cursor.moveToNext()) {
                feedsToSync.add(getContactsFeedForGroup(account, cursor.getString(0)));
            }
        } finally {
            cursor.close();
        }
    }

    private static boolean getShouldSyncEverything(ContentResolver cr, String account) {
        String value = Contacts.Settings.getSetting(cr, account, Contacts.Settings.SYNC_EVERYTHING);
        return !TextUtils.isEmpty(value) && !"0".equals(value);
    }

    private void getServerPhotos(SyncContext context, String feedUrl, int maxDownloads,
            GDataSyncData syncData, SyncResult syncResult) {
        final ContentResolver cr = getContext().getContentResolver();
        Cursor cursor = cr.query(
                Photos.CONTENT_URI,
                new String[]{Photos._SYNC_ID, Photos._SYNC_VERSION, Photos.PERSON_ID,
                        Photos.DOWNLOAD_REQUIRED, Photos._ID}, ""
                + "_sync_account=? AND download_required != 0",
                new String[]{getAccount()}, null);
        try {
            int numFetched = 0;
            while (cursor.moveToNext()) {
                if (numFetched >= maxDownloads) {
                    break;
                }
                String photoSyncId = cursor.getString(0);
                String photoVersion = cursor.getString(1);
                long person = cursor.getLong(2);
                String photoUrl = feedUrl + "/" + photoSyncId;
                long photoId = cursor.getLong(4);

                try {
                    context.setStatusText("Downloading photo " + photoSyncId);
                    ++numFetched;
                    ++mPhotoDownloads;
                    InputStream inputStream = mContactsClient.getMediaEntryAsStream(
                            photoUrl, getAuthToken());
                    savePhoto(person, inputStream, photoVersion);
                    syncResult.stats.numUpdates++;
                } catch (IOException e) {
                    if (Log.isLoggable(TAG, Log.VERBOSE)) {
                        Log.d(TAG, "error downloading " + photoUrl, e);
                    }
                    syncResult.stats.numIoExceptions++;
                    return;
                } catch (HttpException e) {
                    switch (e.getStatusCode()) {
                        case HttpException.SC_UNAUTHORIZED:
                            if (Config.LOGD) {
                                Log.d(TAG, "not authorized to download " + photoUrl, e);
                            }
                            syncResult.stats.numAuthExceptions++;
                            return;
                        case HttpException.SC_FORBIDDEN:
                        case HttpException.SC_NOT_FOUND:
                            final String exceptionMessage = e.getMessage();
                            if (Config.LOGD) {
                                Log.d(TAG, "unable to download photo " + photoUrl + ", "
                                        + exceptionMessage + ", ignoring");
                            }
                            ContentValues values = new ContentValues();
                            values.put(Photos.SYNC_ERROR, exceptionMessage);
                            Uri photoUri = Uri.withAppendedPath(
                                    ContentUris.withAppendedId(People.CONTENT_URI, photoId),
                                    Photos.CONTENT_DIRECTORY);
                            cr.update(photoUri, values, null /* where */, null /* where args */);
                            break;
                        default:
                            if (Config.LOGD) {
                                Log.d(TAG, "error downloading " + photoUrl, e);
                            }
                            syncResult.stats.numIoExceptions++;
                            return;
                    }
                }
            }
            final boolean hasMoreToSync = numFetched < cursor.getCount();
            GDataSyncData.FeedData feedData =
                    new GDataSyncData.FeedData(0  /* no update time */,
                            numFetched, hasMoreToSync, null /* no lastId */,
                            0 /* no feed index */);
            syncData.feedData.put(feedUrl, feedData);
        } finally {
            cursor.close();
        }
    }

    @Override
    protected void getStatsString(StringBuffer sb, SyncResult result) {
        super.getStatsString(sb, result);
        if (mPhotoUploads > 0) {
            sb.append("p").append(mPhotoUploads);
        }
        if (mPhotoDownloads > 0) {
            sb.append("P").append(mPhotoDownloads);
        }
    }

    @Override
    public void sendClientDiffs(SyncContext context, SyncableContentProvider clientDiffs,
            SyncableContentProvider serverDiffs, SyncResult syncResult,
            boolean dontSendDeletes) {
        initTempProvider(clientDiffs);

        sendClientDiffsImpl(context, clientDiffs, new GroupEntry(), null /* no syncInfo */,
                serverDiffs, syncResult, dontSendDeletes);

        // lets go ahead and commit what we have if we successfully made a change
        if (syncResult.madeSomeProgress()) {
            return;
        }

        sendClientPhotos(context, clientDiffs, null /* no syncInfo */, syncResult);

        // lets go ahead and commit what we have if we successfully made a change
        if (syncResult.madeSomeProgress()) {
            return;
        }

        sendClientDiffsImpl(context, clientDiffs, new ContactEntry(), null /* no syncInfo */,
                serverDiffs, syncResult, dontSendDeletes);
    }

    protected void sendClientPhotos(SyncContext context, ContentProvider clientDiffs,
            Object syncInfo, SyncResult syncResult) {
        Entry entry = new MediaEntry();

        GDataServiceClient client = getGDataServiceClient();
        String authToken = getAuthToken();
        ContentResolver cr = getContext().getContentResolver();
        final String account = getAccount();

        Cursor c = clientDiffs.query(Photos.CONTENT_URI, null /* all columns */,
                null /* no where */, null /* no where args */, null /* default sort order */);
        try {
            int personColumn = c.getColumnIndexOrThrow(Photos.PERSON_ID);
            int dataColumn = c.getColumnIndexOrThrow(Photos.DATA);
            int numRows = c.getCount();
            while (c.moveToNext()) {
                if (mSyncCanceled) {
                    if (Config.LOGD) Log.d(TAG, "stopping since the sync was canceled");
                    break;
                }

                entry.clear();
                context.setStatusText("Updating, " + (numRows - 1) + " to go");

                cursorToBaseEntry(entry, account, c);
                String editUrl = entry.getEditUri();

                if (TextUtils.isEmpty(editUrl)) {
                    if (Config.LOGD) {
                        Log.d(TAG, "skipping photo edit for unsynced contact");
                    }
                    continue;
                }

                // Send the request and receive the response
                InputStream inputStream = null;
                byte[] imageData = c.getBlob(dataColumn);
                if (imageData != null) {
                    inputStream = new ByteArrayInputStream(imageData);
                }
                Uri photoUri = Uri.withAppendedPath(People.CONTENT_URI,
                        c.getString(personColumn) + "/" + Photos.CONTENT_DIRECTORY);
                try {
                    if (inputStream != null) {
                        if (Log.isLoggable(TAG, Log.VERBOSE)) {
                            Log.v(TAG, "Updating photo " + entry.toString());
                        }
                        ++mPhotoUploads;
                        client.updateMediaEntry(editUrl, inputStream, IMAGE_MIME_TYPE, authToken);
                    } else {
                        if (Log.isLoggable(TAG, Log.VERBOSE)) {
                            Log.v(TAG, "Deleting photo " + entry.toString());
                        }
                        client.deleteEntry(editUrl, authToken);
                    }

                    // Mark that this photo is no longer dirty. The next time we sync (which
                    // should be soon), we will get the new version of the photo and whether
                    // or not there is a new one to download (e.g. if we deleted our version
                    // yet there is an evergreen version present).
                    ContentValues values = new ContentValues();
                    values.put(Photos.EXISTS_ON_SERVER, inputStream == null ? 0 : 1);
                    values.put(Photos._SYNC_DIRTY, 0);
                    if (cr.update(photoUri, values,
                            null /* no where */, null /* no where args */) != 1) {
                        Log.e(TAG, "error updating photo " + photoUri + " with values " + values);
                        syncResult.stats.numParseExceptions++;
                    } else {
                        syncResult.stats.numUpdates++;
                    }
                    continue;
                } catch (ParseException e) {
                    Log.e(TAG, "parse error during update of " + ", skipping");
                    syncResult.stats.numParseExceptions++;
                } catch (IOException e) {
                    if (Config.LOGD) {
                        Log.d(TAG, "io error during update of " + entry.toString()
                                + ", skipping");
                    }
                    syncResult.stats.numIoExceptions++;
                } catch (HttpException e) {
                    switch (e.getStatusCode()) {
                        case HttpException.SC_UNAUTHORIZED:
                            if (syncResult.stats.numAuthExceptions == 0) {
                                if (Config.LOGD) {
                                   Log.d(TAG, "auth error during update of " + entry
                                           + ", skipping");
                                }
                            }
                            syncResult.stats.numAuthExceptions++;
                            try {
                                GoogleLoginServiceBlockingHelper.invalidateAuthToken(getContext(),
                                        authToken);
                            } catch (GoogleLoginServiceNotFoundException e1) {
                                if (Config.LOGD) {
                                    Log.d(TAG, "could not invalidate auth token", e1);
                                }
                            }
                            return;

                        case HttpException.SC_CONFLICT:
                            if (Config.LOGD) {
                                Log.d(TAG, "conflict detected during update of " + entry
                                        + ", skipping");
                            }
                            syncResult.stats.numConflictDetectedExceptions++;
                            break;
                        case HttpException.SC_BAD_REQUEST:
                        case HttpException.SC_FORBIDDEN:
                        case HttpException.SC_NOT_FOUND:
                        case HttpException.SC_INTERNAL_SERVER_ERROR:
                        default:
                            if (Config.LOGD) {
                                Log.d(TAG, "error " + e.getMessage() + " during update of "
                                        + entry.toString() + ", skipping");
                            }
                            syncResult.stats.numIoExceptions++;
                    }
                }
            }
        } finally {
            c.close();
        }
    }

    @Override
    protected Cursor getCursorForTable(ContentProvider cp, Class entryClass) {
        return getCursorForTableImpl(cp, entryClass);
    }

    protected static Cursor getCursorForTableImpl(ContentProvider cp, Class entryClass) {
        if (entryClass == ContactEntry.class) {
            return cp.query(People.CONTENT_URI, null, null, null, null);
        }
        if (entryClass == GroupEntry.class) {
            return cp.query(Groups.CONTENT_URI, null, null, null, null);
        }
        throw new IllegalArgumentException("unexpected entry class, " + entryClass.getName());
    }

    @Override
    protected Cursor getCursorForDeletedTable(ContentProvider cp, Class entryClass) {
        return getCursorForDeletedTableImpl(cp, entryClass);
    }

    protected static Cursor getCursorForDeletedTableImpl(ContentProvider cp, Class entryClass) {
        if (entryClass == ContactEntry.class) {
            return cp.query(People.DELETED_CONTENT_URI, null, null, null, null);
        }
        if (entryClass == GroupEntry.class) {
            return cp.query(Groups.DELETED_CONTENT_URI, null, null, null, null);
        }
        throw new IllegalArgumentException("unexpected entry class, " + entryClass.getName());
    }

    @Override
    protected String cursorToEntry(SyncContext context, Cursor c, Entry baseEntry,
            Object syncInfo) throws ParseException {
        return cursorToEntryImpl(getContext().getContentResolver(), c, baseEntry, getAccount());
    }

    static protected String cursorToEntryImpl(ContentResolver cr, Cursor c, Entry entry,
            String account) throws ParseException {
        cursorToBaseEntry(entry, account, c);
        String createUrl = null;
        if (entry instanceof ContactEntry) {
            cursorToContactEntry(account, cr, c, (ContactEntry) entry);
            if (entry.getEditUri() == null) {
                createUrl = getContactsFeedForAccount(account);
            }
        } else if (entry instanceof MediaEntry) {
            if (entry.getEditUri() == null) {
                createUrl = getPhotosFeedForAccount(account);
            }
        } else {
            cursorToGroupEntry(c, (GroupEntry) entry);
            if (entry.getEditUri() == null) {
                createUrl = getGroupsFeedForAccount(account);
            }
        }

        return createUrl;
    }

    private static void cursorToGroupEntry(Cursor c, GroupEntry entry) throws ParseException {
        if (!TextUtils.isEmpty(c.getString(c.getColumnIndexOrThrow(Groups.SYSTEM_ID)))) {
            throw new ParseException("unable to modify system groups");
        }
        entry.setTitle(c.getString(c.getColumnIndexOrThrow(Groups.NAME)));
        entry.setContent(c.getString(c.getColumnIndexOrThrow(Groups.NOTES)));
        entry.setSystemGroup(null);
    }

    private static void cursorToContactEntry(String account, ContentResolver cr, Cursor c,
            ContactEntry entry)
            throws ParseException {
        entry.setTitle(c.getString(c.getColumnIndexOrThrow(People.NAME)));
        entry.setContent(c.getString(c.getColumnIndexOrThrow(People.NOTES)));

        long syncLocalId = c.getLong(c.getColumnIndexOrThrow(SyncConstValue._SYNC_LOCAL_ID));
        addContactMethodsToContactEntry(cr, syncLocalId, entry);
        addPhonesToContactEntry(cr, syncLocalId, entry);
        addOrganizationsToContactEntry(cr, syncLocalId, entry);
        addGroupMembershipToContactEntry(account, cr, syncLocalId, entry);
        addExtensionsToContactEntry(cr, syncLocalId, entry);
    }

    @Override
    protected void deletedCursorToEntry(SyncContext context, Cursor c, Entry entry) {
        deletedCursorToEntryImpl(c, entry, getAccount());
    }

    protected boolean handleAllDeletedUnavailable(GDataSyncData syncData, String feed) {
        // Contacts has no way to clear the contacts for just a given feed so it is unable
        // to handle this condition itself. Instead it returns false, which tell the
        // sync framework that it must handle it.
        return false;
    }

    protected static void deletedCursorToEntryImpl(Cursor c, Entry entry, String account) {
        cursorToBaseEntry(entry, account, c);
    }

    private static void cursorToBaseEntry(Entry entry, String account, Cursor c) {
        String feedUrl;
        if (entry instanceof ContactEntry) {
            feedUrl = getContactsFeedForAccount(account);
        } else if (entry instanceof GroupEntry) {
            feedUrl = getGroupsFeedForAccount(account);
        } else if (entry instanceof MediaEntry) {
            feedUrl = getPhotosFeedForAccount(account);
        } else {
            throw new IllegalArgumentException("bad entry type: " + entry.getClass().getName());
        }

        String syncId = c.getString(c.getColumnIndexOrThrow(SyncConstValue._SYNC_ID));
        if (syncId != null) {
            String syncVersion = c.getString(c.getColumnIndexOrThrow(SyncConstValue._SYNC_VERSION));
            entry.setId(feedUrl + "/" + syncId);
            entry.setEditUri(entry.getId() + "/" + syncVersion);
        }
    }

    private static void addPhonesToContactEntry(ContentResolver cr, long personId, ContactEntry entry)
            throws ParseException {
        Cursor c = cr.query(Phones.CONTENT_URI, null, "person=" + personId, null, null);
        int numberIndex = c.getColumnIndexOrThrow(People.Phones.NUMBER);
        try {
            while (c.moveToNext()) {
                PhoneNumber phoneNumber = new PhoneNumber();
                cursorToContactsElement(phoneNumber, c, PROVIDER_TYPE_TO_ENTRY_PHONE);
                phoneNumber.setPhoneNumber(c.getString(numberIndex));
                entry.addPhoneNumber(phoneNumber);
            }
        } finally {
            if (c != null) c.close();
        }
    }


    static private void addContactMethodsToContactEntry(ContentResolver cr, long personId,
            ContactEntry entry) throws ParseException {
        Cursor c = cr.query(ContactMethods.CONTENT_URI, null,
                "person=" + personId, null, null);
        int kindIndex = c.getColumnIndexOrThrow(ContactMethods.KIND);
        int dataIndex = c.getColumnIndexOrThrow(ContactMethods.DATA);
        int auxDataIndex = c.getColumnIndexOrThrow(ContactMethods.AUX_DATA);
        try {
            while (c.moveToNext()) {
                int kind = c.getInt(kindIndex);
                switch (kind) {
                    case Contacts.KIND_IM: {
                        ImAddress address = new ImAddress();
                        cursorToContactsElement(address, c, PROVIDER_TYPE_TO_ENTRY_IM);
                        address.setAddress(c.getString(dataIndex));
                        Object object = ContactMethods.decodeImProtocol(c.getString(auxDataIndex));
                        if (object == null) {
                            address.setProtocolPredefined(ImAddress.PROTOCOL_NONE);
                        } else if (object instanceof Integer) {
                            address.setProtocolPredefined(
                                    PROVIDER_IM_PROTOCOL_TO_ENTRY_PROTOCOL.get((Integer)object));
                        } else {
                            if (!(object instanceof String)) {
                                throw new IllegalArgumentException("expected an String, " + object);
                            }
                            address.setProtocolPredefined(ImAddress.PROTOCOL_CUSTOM);
                            address.setProtocolCustom((String)object);
                        }
                        entry.addImAddress(address);
                        break;
                    }
                    case Contacts.KIND_POSTAL: {
                        PostalAddress address = new PostalAddress();
                        cursorToContactsElement(address, c, PROVIDER_TYPE_TO_ENTRY_POSTAL);
                        address.setValue(c.getString(dataIndex));
                        entry.addPostalAddress(address);
                        break;
                    }
                    case Contacts.KIND_EMAIL: {
                        EmailAddress address = new EmailAddress();
                        cursorToContactsElement(address, c, PROVIDER_TYPE_TO_ENTRY_EMAIL);
                        address.setAddress(c.getString(dataIndex));
                        entry.addEmailAddress(address);
                        break;
                    }
                }
            }
        } finally {
            if (c != null) c.close();
        }
    }

    private static void addOrganizationsToContactEntry(ContentResolver cr, long personId,
            ContactEntry entry) throws ParseException {
        Cursor c = cr.query(Organizations.CONTENT_URI, null,
                "person=" + personId, null, null);
        try {
            int companyIndex = c.getColumnIndexOrThrow(Organizations.COMPANY);
            int titleIndex = c.getColumnIndexOrThrow(Organizations.TITLE);
            while (c.moveToNext()) {
                Organization organization = new Organization();
                cursorToContactsElement(organization, c, PROVIDER_TYPE_TO_ENTRY_ORGANIZATION);
                organization.setName(c.getString(companyIndex));
                organization.setTitle(c.getString(titleIndex));
                entry.addOrganization(organization);
            }
        } finally {
            if (c != null) c.close();
        }
    }

    private static void addGroupMembershipToContactEntry(String account, ContentResolver cr,
            long personId, ContactEntry entry) throws ParseException {
        Cursor c = cr.query(GroupMembership.RAW_CONTENT_URI, null,
                "person=" + personId, null, null);
        try {
            int serverIdIndex = c.getColumnIndexOrThrow(GroupMembership.GROUP_SYNC_ID);
            int localIdIndex = c.getColumnIndexOrThrow(GroupMembership.GROUP_ID);
            while (c.moveToNext()) {
                String serverId = c.getString(serverIdIndex);
                if (serverId == null) {
                    final Uri groupUri = ContentUris
                            .withAppendedId(Groups.CONTENT_URI, c.getLong(localIdIndex));
                    Cursor groupCursor = cr.query(groupUri, new String[]{Groups._SYNC_ID},
                            null, null, null);
                    try {
                        if (groupCursor.moveToNext()) {
                            serverId = groupCursor.getString(0);
                        }
                    } finally {
                        groupCursor.close();
                    }
                }
                if (serverId == null) {
                    // the group hasn't been synced yet, we can't complete this operation since
                    // we don't know what server id to use for the group
                    throw new ParseException("unable to construct GroupMembershipInfo since the "
                            + "group _sync_id isn't known yet, will retry later");
                }
                GroupMembershipInfo groupMembershipInfo = new GroupMembershipInfo();
                String groupId = getCanonicalGroupsFeedForAccount(account) + "/" + serverId;
                groupMembershipInfo.setGroup(groupId);
                groupMembershipInfo.setDeleted(false);
                entry.addGroup(groupMembershipInfo);
            }
        } finally {
            if (c != null) c.close();
        }
    }

    private static void addExtensionsToContactEntry(ContentResolver cr, long personId,
            ContactEntry entry) throws ParseException {
        Cursor c = cr.query(Extensions.CONTENT_URI, null, "person=" + personId, null, null);
        try {
            JSONObject jsonObject = new JSONObject();
            int nameIndex = c.getColumnIndexOrThrow(Extensions.NAME);
            int valueIndex = c.getColumnIndexOrThrow(Extensions.VALUE);
            if (c.getCount() == 0) return;
            while (c.moveToNext()) {
                try {
                    jsonObject.put(c.getString(nameIndex), c.getString(valueIndex));
                } catch (JSONException e) {
                    throw new ParseException("bad key or value", e);
                }
            }
            ExtendedProperty extendedProperty = new ExtendedProperty();
            extendedProperty.setName("android");
            final String jsonString = jsonObject.toString();
            if (jsonString == null) {
                throw new ParseException("unable to convert cursor into a JSON string, "
                        + DatabaseUtils.dumpCursorToString(c));
            }
            extendedProperty.setXmlBlob(jsonString);
            entry.addExtendedProperty(extendedProperty);
        } finally {
            if (c != null) c.close();
        }
    }

    private static void cursorToContactsElement(ContactsElement element,
            Cursor c, HashMap<Integer, Byte> map) {
        final int typeIndex = c.getColumnIndexOrThrow("type");
        final int labelIndex = c.getColumnIndexOrThrow("label");
        final int isPrimaryIndex = c.getColumnIndexOrThrow("isprimary");

        element.setLabel(c.getString(labelIndex));
        element.setType(map.get(c.getInt(typeIndex)));
        element.setIsPrimary(c.getInt(isPrimaryIndex) != 0);
    }

    private static void contactsElementToValues(ContentValues values, ContactsElement element,
            HashMap<Byte, Integer> map) {
        values.put("type", map.get(element.getType()));
        values.put("label", element.getLabel());
        values.put("isprimary", element.isPrimary() ? 1 : 0);
    }

    /*
     * Takes the entry, casts it to a ContactEntry and executes the appropriate
     * actions on the ContentProvider to represent the entry.
     */
    protected void updateProvider(Feed feed, Long syncLocalId,
            Entry baseEntry, ContentProvider provider, Object syncInfo) throws ParseException {

        // This is a hack to delete these incorrectly created contacts named "Starred in Android"
        if (baseEntry instanceof ContactEntry
                && "Starred in Android".equals(baseEntry.getTitle())) {
            Log.i(TAG, "Deleting incorrectly created contact from the server: " + baseEntry);
            GDataServiceClient client = getGDataServiceClient();
            try {
                client.deleteEntry(baseEntry.getEditUri(), getAuthToken());
            } catch (IOException e) {
                Log.i(TAG, "  exception while deleting contact: " + baseEntry, e);
            } catch (com.google.wireless.gdata.client.HttpException e) {
                Log.i(TAG, "  exception while deleting contact: " + baseEntry, e);
            }
        }

        updateProviderImpl(getAccount(), syncLocalId, baseEntry, provider);
    }

    protected static void updateProviderImpl(String account, Long syncLocalId,
            Entry entry, ContentProvider provider) throws ParseException {
        // If this is a deleted entry then add it to the DELETED_CONTENT_URI
        ContentValues deletedValues = null;
        if (entry.isDeleted()) {
            deletedValues = new ContentValues();
            deletedValues.put(SyncConstValue._SYNC_LOCAL_ID, syncLocalId);
            final String id = entry.getId();
            final String editUri = entry.getEditUri();
            if (!TextUtils.isEmpty(id)) {
                deletedValues.put(SyncConstValue._SYNC_ID, lastItemFromUri(id));
            }
            if (!TextUtils.isEmpty(editUri)) {
                deletedValues.put(SyncConstValue._SYNC_VERSION, lastItemFromUri(editUri));
            }
            deletedValues.put(SyncConstValue._SYNC_ACCOUNT, account);
        }

        if (entry instanceof ContactEntry) {
            if (deletedValues != null) {
                provider.insert(People.DELETED_CONTENT_URI, deletedValues);
                return;
            }
            updateProviderWithContactEntry(account, syncLocalId, (ContactEntry) entry, provider);
            return;
        }
        if (entry instanceof GroupEntry) {
            if (deletedValues != null) {
                provider.insert(Groups.DELETED_CONTENT_URI, deletedValues);
                return;
            }
            updateProviderWithGroupEntry(account, syncLocalId, (GroupEntry) entry, provider);
            return;
        }
        throw new IllegalArgumentException("unknown entry type, " + entry.getClass().getName());
    }

    protected static void updateProviderWithContactEntry(String account, Long syncLocalId,
            ContactEntry entry, ContentProvider provider) throws ParseException {
        final String name = entry.getTitle();
        final String notes = entry.getContent();
        final String personSyncId = lastItemFromUri(entry.getId());
        final String personSyncVersion = lastItemFromUri(entry.getEditUri());

        // Store the info about the person
        ContentValues values = new ContentValues();
        values.put(People.NAME, name);
        values.put(People.NOTES, notes);
        values.put(SyncConstValue._SYNC_ACCOUNT, account);
        values.put(SyncConstValue._SYNC_ID, personSyncId);
        values.put(SyncConstValue._SYNC_DIRTY, "0");
        values.put(SyncConstValue._SYNC_LOCAL_ID, syncLocalId);
        values.put(SyncConstValue._SYNC_TIME, personSyncVersion);
        values.put(SyncConstValue._SYNC_VERSION, personSyncVersion);
        Uri personUri = provider.insert(People.CONTENT_URI, values);

        // Store the photo information
        final boolean photoExistsOnServer = !TextUtils.isEmpty(entry.getLinkPhotoHref());
        final String photoVersion = lastItemFromUri(entry.getLinkEditPhotoHref());
        values.clear();
        values.put(Photos.PERSON_ID, ContentUris.parseId(personUri));
        values.put(Photos.EXISTS_ON_SERVER, photoExistsOnServer ? 1 : 0);
        values.put(SyncConstValue._SYNC_ACCOUNT, account);
        values.put(SyncConstValue._SYNC_ID, personSyncId);
        values.put(SyncConstValue._SYNC_DIRTY, 0);
        values.put(SyncConstValue._SYNC_LOCAL_ID, syncLocalId);
        values.put(SyncConstValue._SYNC_TIME, photoVersion);
        values.put(SyncConstValue._SYNC_VERSION, photoVersion);
        if (provider.insert(Photos.CONTENT_URI, values) == null) {
            Log.e(TAG, "error inserting photo row, " + values);
        }

        // Store each email address
        for (Object object : entry.getEmailAddresses()) {
            EmailAddress email = (EmailAddress) object;
            values.clear();
            contactsElementToValues(values, email, ENTRY_TYPE_TO_PROVIDER_EMAIL);
            values.put(ContactMethods.DATA, email.getAddress());
            values.put(ContactMethods.KIND, Contacts.KIND_EMAIL);
            Uri uri = Uri.withAppendedPath(personUri, People.ContactMethods.CONTENT_DIRECTORY);
            provider.insert(uri, values);
        }

        // Store each postal address
        for (Object object : entry.getPostalAddresses()) {
            PostalAddress address = (PostalAddress) object;
            values.clear();
            contactsElementToValues(values, address, ENTRY_TYPE_TO_PROVIDER_POSTAL);
            values.put(ContactMethods.DATA, address.getValue());
            values.put(ContactMethods.KIND, Contacts.KIND_POSTAL);
            Uri uri = Uri.withAppendedPath(personUri, People.ContactMethods.CONTENT_DIRECTORY);
            provider.insert(uri, values);
        }

        // Store each im address
        for (Object object : entry.getImAddresses()) {
            ImAddress address = (ImAddress) object;
            values.clear();
            contactsElementToValues(values, address, ENTRY_TYPE_TO_PROVIDER_IM);
            values.put(ContactMethods.DATA, address.getAddress());
            values.put(ContactMethods.KIND, Contacts.KIND_IM);
            final byte protocolType = address.getProtocolPredefined();
            if (protocolType == ImAddress.PROTOCOL_NONE) {
                // don't add anything
            } else if (protocolType == ImAddress.PROTOCOL_CUSTOM) {
                values.put(ContactMethods.AUX_DATA,
                        ContactMethods.encodeCustomImProtocol(address.getProtocolCustom()));
            } else {
                Integer providerProtocolType =
                        ENTRY_IM_PROTOCOL_TO_PROVIDER_PROTOCOL .get(protocolType);
                if (providerProtocolType == null) {
                    throw new IllegalArgumentException("unknown protocol type, " + protocolType);
                }
                values.put(ContactMethods.AUX_DATA,
                        ContactMethods.encodePredefinedImProtocol(providerProtocolType));
            }
            Uri uri = Uri.withAppendedPath(personUri, People.ContactMethods.CONTENT_DIRECTORY);
            provider.insert(uri, values);
        }

        // Store each organization
        for (Object object : entry.getOrganizations()) {
            Organization organization = (Organization) object;
            values.clear();
            contactsElementToValues(values, organization, ENTRY_TYPE_TO_PROVIDER_ORGANIZATION);
            values.put(Organizations.COMPANY, organization.getName());
            values.put(Organizations.TITLE, organization.getTitle());
            values.put(Organizations.COMPANY, organization.getName());
            Uri uri = Uri.withAppendedPath(personUri, Organizations.CONTENT_DIRECTORY);
            provider.insert(uri, values);
        }

        // Store each group
        for (Object object : entry.getGroups()) {
            GroupMembershipInfo groupMembershipInfo = (GroupMembershipInfo) object;
            if (groupMembershipInfo.isDeleted()) {
                continue;
            }
            values.clear();
            values.put(GroupMembership.GROUP_SYNC_ACCOUNT, account);
            values.put(GroupMembership.GROUP_SYNC_ID,
                    lastItemFromUri(groupMembershipInfo.getGroup()));
            Uri uri = Uri.withAppendedPath(personUri, GroupMembership.CONTENT_DIRECTORY);
            provider.insert(uri, values);
        }

        // Store each phone number
        for (Object object : entry.getPhoneNumbers()) {
            PhoneNumber phone = (PhoneNumber) object;
            values.clear();
            contactsElementToValues(values, phone, ENTRY_TYPE_TO_PROVIDER_PHONE);
            values.put(People.Phones.NUMBER, phone.getPhoneNumber());
            values.put(People.Phones.LABEL, phone.getLabel());
            Uri uri = Uri.withAppendedPath(personUri, People.Phones.CONTENT_DIRECTORY);
            provider.insert(uri, values);
        }

        // Store the extended properties
        for (Object object : entry.getExtendedProperties()) {
            ExtendedProperty extendedProperty = (ExtendedProperty) object;
            if (!"android".equals(extendedProperty.getName())) {
                continue;
            }
            JSONObject jsonObject = null;
            try {
                jsonObject = new JSONObject(extendedProperty.getXmlBlob());
            } catch (JSONException e) {
                Log.w(TAG, "error parsing the android extended property, dropping, entry is "
                        + entry.toString());
                continue;
            }
            Iterator jsonIterator = jsonObject.keys();
            while (jsonIterator.hasNext()) {
                String key = (String)jsonIterator.next();
                values.clear();
                values.put(Extensions.NAME, key);
                try {
                    values.put(Extensions.VALUE, jsonObject.getString(key));
                } catch (JSONException e) {
                    // this should never happen, since we just got the key from the iterator
                }
                Uri uri = Uri.withAppendedPath(personUri, People.Extensions.CONTENT_DIRECTORY);
                if (null == provider.insert(uri, values)) {
                    Log.e(TAG, "Error inserting extension into provider, uri "
                            + uri + ", values " + values);
                }
            }
            break;
        }
    }

    protected static void updateProviderWithGroupEntry(String account, Long syncLocalId,
            GroupEntry entry, ContentProvider provider) throws ParseException {
        ContentValues values = new ContentValues();
        values.put(Groups.NAME, entry.getTitle());
        values.put(Groups.NOTES, entry.getContent());
        values.put(Groups.SYSTEM_ID, entry.getSystemGroup());
        values.put(Groups._SYNC_ACCOUNT, account);
        values.put(Groups._SYNC_ID, lastItemFromUri(entry.getId()));
        values.put(Groups._SYNC_DIRTY, 0);
        values.put(Groups._SYNC_LOCAL_ID, syncLocalId);
        final String editUri = entry.getEditUri();
        final String syncVersion = editUri == null ? null : lastItemFromUri(editUri);
        values.put(Groups._SYNC_TIME, syncVersion);
        values.put(Groups._SYNC_VERSION, syncVersion);
        provider.insert(Groups.CONTENT_URI, values);
    }

    private static String lastItemFromUri(String url) {
        return url.substring(url.lastIndexOf('/') + 1);
    }

    protected void savePhoto(long person, InputStream photoInput, String photoVersion)
            throws IOException {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            byte[] data = new byte[1024];
            while(true) {
                int bytesRead = photoInput.read(data);
                if (bytesRead < 0) break;
                byteStream.write(data, 0, bytesRead);
            }

            ContentValues values = new ContentValues();
            // we have to include this here otherwise the provider will set it to 1
            values.put(Photos._SYNC_DIRTY, 0);
            values.put(Photos.LOCAL_VERSION, photoVersion);
            values.put(Photos.DATA, byteStream.toByteArray());
            Uri photoUri = Uri.withAppendedPath(People.CONTENT_URI,
                    "" + person + "/" + Photos.CONTENT_DIRECTORY);
            if (getContext().getContentResolver().update(photoUri, values,
                    "_sync_dirty=0", null) > 0) {
                if (Log.isLoggable(TAG, Log.VERBOSE)) {
                    Log.v(TAG, "savePhoto: updated " + photoUri + " with values " + values);
                }
            } else {
                Log.e(TAG, "savePhoto: update of " + photoUri + " with values " + values
                        + " affected no rows");
            }
        } finally {
            try {
                if (photoInput != null) photoInput.close();
            } catch (IOException e) {
                // we don't care about exceptions here
            }
        }
    }

    /**
     * Make sure the contacts subscriptions we expect based on the current
     * accounts are present and that there aren't any extra subscriptions
     * that we don't expect.
     */
    @Override
    public void onAccountsChanged(String[] accountsArray) {
        if (!"yes".equals(SystemProperties.get("ro.config.sync"))) {
            return;
        }

        ContentResolver cr = getContext().getContentResolver();
        for (String account : accountsArray) {
            String value = Contacts.Settings.getSetting(cr, account,
                    Contacts.Settings.SYNC_EVERYTHING);
            if (value == null) {
                Contacts.Settings.setSetting(cr, account, Contacts.Settings.SYNC_EVERYTHING, "1");
            }
            updateSubscribedFeeds(cr, account);
        }
    }

    /**
     *  Returns the contacts feed url for a specific account.
     *  @param account The account
     *  @return The contacts feed url for a specific account.
     */
    public static String getContactsFeedForAccount(String account) {
        String url = CONTACTS_FEED_URL + account + "/base2_property-android";
        return rewriteUrlforAccount(account, url);
    }

    /**
     *  Returns the contacts group feed url for a specific account.
     *  @param account The account
     *  @param groupSyncId The group id
     *  @return The contacts feed url for a specific account and group.
     */
    public static String getContactsFeedForGroup(String account, String groupSyncId) {
        String groupId = getCanonicalGroupsFeedForAccount(account);
        try {
            groupId = URLEncoder.encode(groupId, "utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("unable to url encode group: " + groupId);
        }
        return getContactsFeedForAccount(account) + "?group=" + groupId + "/" + groupSyncId;
    }

    /**
     *  Returns the groups feed url for a specific account.
     *  @param account The account
     *  @return The groups feed url for a specific account.
     */
    public static String getGroupsFeedForAccount(String account) {
        String url = GROUPS_FEED_URL + account + "/base2_property-android";
        return rewriteUrlforAccount(account, url);
    }

    /**
     *  Returns the groups feed url for a specific account that should be
     *  used as the foreign reference to this group, e.g. in the
     *  group membership element of the ContactEntry. The canonical groups
     *  feed always uses http (so it doesn't need to be rewritten) and it always
     *  uses the base projection. 
     *  @param account The account
     *  @return The groups feed url for a specific account.
     */
    public static String getCanonicalGroupsFeedForAccount(String account) {
        return GROUPS_FEED_URL + account + "/base";
    }

    /**
     *  Returns the photo feed url for a specific account.
     *  @param account The account
     *  @return The photo feed url for a specific account.
     */
    public static String getPhotosFeedForAccount(String account) {
        String url = PHOTO_FEED_URL + account;
        return rewriteUrlforAccount(account, url);
    }

    protected static boolean getFeedReturnsPartialDiffs() {
        return true;
    }

    @Override
    protected void updateQueryParameters(QueryParams params) {
        // we want to get the events ordered by last modified, so we can
        // recover in case we cannot process the entire feed.
        params.setParamValue("orderby", "lastmodified");
        params.setParamValue("sortorder", "ascending");

        // set showdeleted so that we get tombstones, only do this when we
        // are doing an incremental sync
        if (params.getUpdatedMin() != null) {
            params.setParamValue("showdeleted", "true");
        }
    }

    @Override
    public void onSyncStarting(SyncContext context, String account, boolean forced,
            SyncResult result) {
        mPerformedGetServerDiffs = false;
        mSyncForced = forced;
        mPhotoDownloads = 0;
        mPhotoUploads = 0;
        super.onSyncStarting(context, account, forced, result);
    }

    @Override
    public void onSyncEnding(SyncContext context, boolean success) {
        final ContentResolver cr = getContext().getContentResolver();

        if (success && mPerformedGetServerDiffs && !mSyncCanceled) {
            Cursor cursor = cr.query(
                    Photos.CONTENT_URI,
                    new String[]{Photos._SYNC_ID, Photos._SYNC_VERSION, Photos.PERSON_ID,
                            Photos.DOWNLOAD_REQUIRED}, ""
                    + "_sync_account=? AND download_required != 0",
                    new String[]{getAccount()}, null);
            try {
                if (cursor.getCount() != 0) {
                    Bundle extras = new Bundle();
                    extras.putString(ContentResolver.SYNC_EXTRAS_ACCOUNT, getAccount());
                    extras.putBoolean(ContentResolver.SYNC_EXTRAS_FORCE, mSyncForced);
                    extras.putString("feed",
                            ContactsSyncAdapter.getPhotosFeedForAccount(getAccount()));
                    getContext().getContentResolver().startSync(Contacts.CONTENT_URI, extras);
                }
            } finally {
                cursor.close();
            }
        }

        super.onSyncEnding(context, success);
    }

    public static void updateSubscribedFeeds(ContentResolver cr, String account) {
        Set<String> feedsToSync = Sets.newHashSet();
        feedsToSync.add(getGroupsFeedForAccount(account));
        addContactsFeedsToSync(cr, account, feedsToSync);

        Cursor c = SubscribedFeeds.Feeds.query(cr, sSubscriptionProjection,
                SubscribedFeeds.Feeds.AUTHORITY + "=? AND "
                        + SubscribedFeeds.Feeds._SYNC_ACCOUNT + "=?",
                new String[]{Contacts.AUTHORITY, account}, null);
        try {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "scanning over subscriptions with authority "
                        + Contacts.AUTHORITY + " and account " + account);
            }
            c.moveToNext();
            while (!c.isAfterLast()) {
                String feedInCursor = c.getString(1);
                if (feedsToSync.contains(feedInCursor)) {
                    feedsToSync.remove(feedInCursor);
                    c.moveToNext();
                } else {
                    c.deleteRow();
                }
            }
            c.commitUpdates();
        } finally {
            c.close();
        }

        // any feeds remaining in feedsToSync need a subscription
        for (String feed : feedsToSync) {
            SubscribedFeeds.addFeed(cr, feed, account, Contacts.AUTHORITY, ContactsClient.SERVICE);

            // request a sync of this feed
            Bundle extras = new Bundle();
            extras.putString(ContentResolver.SYNC_EXTRAS_ACCOUNT, account);
            extras.putString("feed", feed);
            cr.startSync(Contacts.CONTENT_URI, extras);
        }
    }
}
