package com.android.providers.contacts;

import com.google.wireless.gdata.contacts.data.ContactEntry;
import com.google.wireless.gdata.contacts.data.EmailAddress;
import com.google.wireless.gdata.contacts.data.GroupEntry;
import com.google.wireless.gdata.contacts.data.GroupMembershipInfo;
import com.google.wireless.gdata.contacts.data.ImAddress;
import com.google.wireless.gdata.contacts.data.Organization;
import com.google.wireless.gdata.contacts.data.PhoneNumber;
import com.google.wireless.gdata.contacts.data.PostalAddress;
import com.google.wireless.gdata.data.Entry;
import com.google.wireless.gdata.data.ExtendedProperty;
import com.google.wireless.gdata.parser.ParseException;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.SyncContext;
import android.content.SyncResult;
import android.content.TempProviderSyncResult;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.net.Uri;
import android.provider.Contacts;
import android.provider.Contacts.ContactMethods;
import android.provider.Contacts.GroupMembership;
import android.provider.Contacts.Groups;
import android.provider.Contacts.People;
import android.provider.Contacts.Photos;
import android.provider.SyncConstValue;
import android.test.ProviderTestCase;
import android.util.Log;
import android.accounts.Account;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ContactsSyncAdapterTest extends ProviderTestCase<ContactsProvider> {
    private static final Account ACCOUNT = new Account("testaccount@example.com", "example.type");

    private MockSyncContext mMockSyncContext = new MockSyncContext();
    private ContactsSyncAdapter mSyncAdapter = null;

    public ContactsSyncAdapterTest() {
        super(ContactsProvider.class, Contacts.AUTHORITY);
    }

    public void testUpdateProviderPeople() throws ParseException {
        final ContactsProvider serverDiffs = newTemporaryProvider();

        ContactEntry entry1 = newPerson("title1", "note1", "1", "a");
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, 11L, entry1, serverDiffs);

        ContactEntry entry2 = newPerson("title2", "note2", "2", "b");
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, null, entry2, serverDiffs);

        ContactEntry entry3 = newPerson("title3", "note3", "3", "c");
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, 13L, entry3, serverDiffs);

        ContactEntry entry4 = newPerson("title4", "note4", "4", "d");
        entry4.setDeleted(true);
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, 14L, entry4, serverDiffs);

        ContactEntry entry5 = newDeletedPerson("5", "e");
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, 15L, entry5, serverDiffs);

        checkTableIsEmpty(getProvider(), Contacts.ContactMethods.CONTENT_URI);
        checkTableIsEmpty(getProvider(), Contacts.Organizations.CONTENT_URI);
        checkTableIsEmpty(getProvider(), Contacts.Phones.CONTENT_URI);

        checkEntries(serverDiffs, false, entry1, 11L, entry2, null, entry3, 13L);
        checkEntries(serverDiffs, true, entry4, 14L, entry5, 15L);

        // Convert the provider back to an entry and check that they match
        Cursor cursor;
        cursor = ContactsSyncAdapter.getCursorForTableImpl(serverDiffs, ContactEntry.class);
        try {
            checkNextCursorToEntry(cursor, entry1);
            checkNextCursorToEntry(cursor, entry2);
            checkNextCursorToEntry(cursor, entry3);
            assertTrue(dumpCursor(cursor), cursor.isLast());
        } finally {
            cursor.close();
        }

        cursor = ContactsSyncAdapter.getCursorForDeletedTableImpl(serverDiffs, ContactEntry.class);
        try {
            checkNextDeletedCursorToEntry(cursor, entry4);
            checkNextDeletedCursorToEntry(cursor, entry5);
            assertTrue(dumpCursor(cursor), cursor.isLast());
        } finally {
            cursor.close();
        }
    }

    public void testUpdateProviderGroups() throws ParseException {
        final ContactsProvider serverDiffs = newTemporaryProvider();

        GroupEntry g1 = newGroup("g1", "n1", "i1", "v1");
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, 21L, g1, serverDiffs);

        GroupEntry g2 = newGroup("g2", "n2", "i2", "v2");
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, null, g2, serverDiffs);

        GroupEntry g3 = newGroup("g3", "n3", "i3", "v3");
        g3.setDeleted(true);
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, 23L, g3, serverDiffs);

        GroupEntry g4 = newDeletedGroup("i4", "v4");
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, 24L, g4, serverDiffs);

        // confirm that the entries we expect are in the Groups and DeletedGroups
        // tables
        checkEntries(serverDiffs, false, g1, 21L, g2, null);
        checkEntries(serverDiffs, true, g3, 23L, g4, 24L);

        // Convert the provider back to an entry and check that they match
        Cursor cursor;
        cursor = ContactsSyncAdapter.getCursorForTableImpl(serverDiffs, GroupEntry.class);
        try {
            checkNextCursorToEntry(cursor, g1);
            checkNextCursorToEntry(cursor, g2);
            assertTrue(dumpCursor(cursor), cursor.isLast());
        } finally {
            cursor.close();
        }

        cursor = ContactsSyncAdapter.getCursorForDeletedTableImpl(serverDiffs, GroupEntry.class);
        try {
            checkNextDeletedCursorToEntry(cursor, g3);
            checkNextDeletedCursorToEntry(cursor, g4);
            assertTrue(dumpCursor(cursor), cursor.isLast());
        } finally {
            cursor.close();
        }
    }

    // The temp provider contains groups that mirror the GroupEntries
    // The temp provider contains people rows that mirrow the ContactEntry people portion
    // The temp provider contains groupmembership rows that mirror the group membership info
    // These groupmembership rows have people._id as a foreign key
    //   and a group _sync_id, but not as a foreign key
    
    public void testUpdateProviderGroupMembership() throws ParseException {
        final ContactsProvider serverDiffs = newTemporaryProvider();

        ContactEntry p = newPerson("p1", "pn1", "pi1", "pv1");

        addGroupMembership(p, ACCOUNT, "gsi1");
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, 31L, p, serverDiffs);

        // The provider should now have:
        // - a row in the people table
        // - a row in the groupmembership table

        checkEntries(serverDiffs, false, p, 31L);

        checkTableIsEmpty(serverDiffs, ContactsProvider.sPeopleTable);
        checkTableIsEmpty(serverDiffs, ContactsProvider.sGroupmembershipTable);

        // copy the server diffs into the provider
        getProvider().merge(mMockSyncContext, serverDiffs, null, new SyncResult());
        getProvider().getDatabase().execSQL("UPDATE people SET _sync_dirty=1");
        ContentProvider clientDiffs = getClientDiffs();

        // Convert the provider back to an entry and check that they match
        Cursor cursor = ContactsSyncAdapter.getCursorForTableImpl(clientDiffs, ContactEntry.class);
        try {
            checkNextCursorToEntry(cursor, p);
            assertTrue(dumpCursor(cursor), cursor.isLast());
        } finally {
            cursor.close();
        }
    }

    private static void addGroupMembership(ContactEntry p, Account account, String groupId) {
        GroupMembershipInfo groupInfo = new GroupMembershipInfo();
        final String serverId =
                ContactsSyncAdapter.getCanonicalGroupsFeedForAccount(account) + "/" + groupId;
        groupInfo.setGroup(serverId);
        p.addGroup(groupInfo);
    }

    public void testUpdateProviderContactMethods() throws ParseException {
        final String entryTitle = "title1";
        ContactEntry entry = newPerson(entryTitle, "note1", "2", "3");

        addEmail(entry, "a11111", false, EmailAddress.TYPE_HOME, null);
        addEmail(entry, "a22222", true, EmailAddress.TYPE_WORK, null);
        addEmail(entry, "a33333", false, EmailAddress.TYPE_OTHER, null);
        addEmail(entry, "a44444", false, EmailAddress.TYPE_NONE, "lucky");

        addPostal(entry, "b11111", false, PostalAddress.TYPE_HOME, null);
        addPostal(entry, "b22222", false, PostalAddress.TYPE_WORK, null);
        addPostal(entry, "b33333", true, PostalAddress.TYPE_OTHER, null);
        addPostal(entry, "b44444", false, PostalAddress.TYPE_NONE, "lucky");

        addIm(entry, "c11111", ImAddress.PROTOCOL_CUSTOM, "p1", false, ImAddress.TYPE_HOME, null);
        addIm(entry, "c22222", ImAddress.PROTOCOL_NONE, null, true, ImAddress.TYPE_WORK, null);
        addIm(entry, "c33333", ImAddress.PROTOCOL_SKYPE, null, false, ImAddress.TYPE_OTHER, null);
        addIm(entry, "c44444", ImAddress.PROTOCOL_ICQ, null, false, ImAddress.TYPE_NONE, "l2");

        final ContactsProvider provider = newTemporaryProvider();
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, null, entry, provider);

        checkTableIsEmpty(getProvider(), Contacts.Phones.CONTENT_URI);
        checkTableIsEmpty(getProvider(), Contacts.Organizations.CONTENT_URI);

        long personId = getSinglePersonId(provider, entryTitle);

        Cursor cursor;
        cursor = provider.query(Contacts.ContactMethods.CONTENT_URI, null, null, null,
                Contacts.ContactMethods.DATA);
        try {
            checkNextEmail(cursor, personId, "a11111",
                    false, Contacts.ContactMethods.TYPE_HOME, null);
            checkNextEmail(cursor, personId, "a22222",
                    true, Contacts.ContactMethods.TYPE_WORK, null);
            checkNextEmail(cursor, personId, "a33333",
                    false, Contacts.ContactMethods.TYPE_OTHER, null);
            checkNextEmail(cursor, personId, "a44444",
                    false, Contacts.ContactMethods.TYPE_CUSTOM, "lucky");

            checkNextPostal(cursor, personId, "b11111",
                    false, Contacts.ContactMethods.TYPE_HOME, null);
            checkNextPostal(cursor, personId, "b22222",
                    false, Contacts.ContactMethods.TYPE_WORK, null);
            checkNextPostal(cursor, personId, "b33333",
                    true, Contacts.ContactMethods.TYPE_OTHER, null);
            checkNextPostal(cursor, personId, "b44444",
                    false, Contacts.ContactMethods.TYPE_CUSTOM, "lucky");

            checkNextIm(cursor, personId, "c11111", ContactMethods.encodeCustomImProtocol("p1"),
                    false, Contacts.ContactMethods.TYPE_HOME, null);
            checkNextIm(cursor, personId, "c22222", null,
                    true, Contacts.ContactMethods.TYPE_WORK, null);
            checkNextIm(cursor, personId, "c33333",
                    ContactMethods.encodePredefinedImProtocol(ContactMethods.PROTOCOL_SKYPE),
                    false, Contacts.ContactMethods.TYPE_OTHER, null);
            checkNextIm(cursor, personId, "c44444",
                    ContactMethods.encodePredefinedImProtocol(ContactMethods.PROTOCOL_ICQ),
                    false, Contacts.ContactMethods.TYPE_CUSTOM, "l2");

            assertTrue(dumpCursor(cursor), cursor.isLast());
        } finally {
            cursor.close();
        }

        checkCursorToEntry(provider, entry);
    }

    public void testUpdateProviderPhones() throws ParseException {
        final String entryTitle = "title1";
        ContactEntry entry = newPerson(entryTitle, "note1", "2", "3");
        addPhoneNumber(entry, "11111", false, PhoneNumber.TYPE_HOME, null);
        addPhoneNumber(entry, "22222", false, PhoneNumber.TYPE_HOME_FAX, null);
        addPhoneNumber(entry, "33333", false, PhoneNumber.TYPE_MOBILE, null);
        addPhoneNumber(entry, "44444", true, PhoneNumber.TYPE_PAGER, null);
        addPhoneNumber(entry, "55555", false, PhoneNumber.TYPE_WORK, null);
        addPhoneNumber(entry, "66666", false, PhoneNumber.TYPE_WORK_FAX, null);
        addPhoneNumber(entry, "77777", false, PhoneNumber.TYPE_OTHER, null);
        addPhoneNumber(entry, "88888", false, PhoneNumber.TYPE_NONE, "lucky");
        final ContactsProvider provider = newTemporaryProvider();
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, null, entry, provider);

        checkTableIsEmpty(getProvider(), Contacts.ContactMethods.CONTENT_URI);
        checkTableIsEmpty(getProvider(), Contacts.Organizations.CONTENT_URI);

        long personId = getSinglePersonId(provider, entryTitle);

        Cursor cursor;
        cursor = provider.query(Contacts.Phones.CONTENT_URI, null, null, null,
                Contacts.Phones.NUMBER);
        try {
            checkNextNumber(cursor, personId, "11111", false, Contacts.Phones.TYPE_HOME, null);
            checkNextNumber(cursor, personId, "22222", false, Contacts.Phones.TYPE_FAX_HOME, null);
            checkNextNumber(cursor, personId, "33333", false, Contacts.Phones.TYPE_MOBILE, null);
            checkNextNumber(cursor, personId, "44444", true, Contacts.Phones.TYPE_PAGER, null);
            checkNextNumber(cursor, personId, "55555", false, Contacts.Phones.TYPE_WORK, null);
            checkNextNumber(cursor, personId, "66666", false, Contacts.Phones.TYPE_FAX_WORK, null);
            checkNextNumber(cursor, personId, "77777", false, Contacts.Phones.TYPE_OTHER, null);
            checkNextNumber(cursor, personId, "88888", false, Contacts.Phones.TYPE_CUSTOM, "lucky");
            assertTrue(dumpCursor(cursor), cursor.isLast());
        } finally {
            cursor.close();
        }

        checkCursorToEntry(provider, entry);
    }

    public void testUpdateProviderOrganization() throws ParseException {
        final String entryTitle = "title1";
        ContactEntry entry = newPerson(entryTitle, "note1", "2", "3");
        addOrganization(entry, "11111", "title1", true, Organization.TYPE_WORK, null);
        addOrganization(entry, "22222", "title2", false, Organization.TYPE_OTHER, null);
        addOrganization(entry, "33333", "title3", false, Organization.TYPE_NONE, "label1");
        final ContactsProvider provider = newTemporaryProvider();
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, null, entry, provider);

        checkTableIsEmpty(getProvider(), Contacts.ContactMethods.CONTENT_URI);
        checkTableIsEmpty(getProvider(), Contacts.Phones.CONTENT_URI);

        long personId = getSinglePersonId(provider, entryTitle);

        Cursor cursor;
        cursor = provider.query(Contacts.Organizations.CONTENT_URI, null, null, null,
                Contacts.Organizations.COMPANY);
        try {
            checkNextOrganization(cursor, personId, "11111", "title1", true,
                    Contacts.Organizations.TYPE_WORK, null);
            checkNextOrganization(cursor, personId, "22222", "title2", false,
                    Contacts.Organizations.TYPE_OTHER, null);
            checkNextOrganization(cursor, personId, "33333", "title3", false,
                    Contacts.Organizations.TYPE_CUSTOM, "label1");
            assertTrue(dumpCursor(cursor), cursor.isLast());
        } finally {
            cursor.close();
        }

        checkCursorToEntry(provider, entry);
    }

    public void testUpdateProviderExtensions() throws ParseException {
        final String entryTitle = "title1";
        ContactEntry entry = newPerson(entryTitle, "note1", "2", "3");
        addExtendedProperty(entry, "android", null, "{\"other\":\"that\",\"more\":\"hello.mp3\"}");
        final ContactsProvider provider = newTemporaryProvider();
        ContactsSyncAdapter.updateProviderImpl(ACCOUNT, null, entry, provider);

        long personId = getSinglePersonId(provider, entryTitle);

        Cursor cursor;
        cursor = provider.query(Contacts.Extensions.CONTENT_URI, null, null, null,
                Contacts.Extensions.NAME);
        try {
            checkNextExtension(cursor, personId, "more", "hello.mp3");
            checkNextExtension(cursor, personId, "other", "that");
            assertTrue(dumpCursor(cursor), cursor.isLast());
        } finally {
            cursor.close();
        }

        checkCursorToEntry(provider, entry);
    }

    // test writing a photo via the content resolver
    public void testPhotoAccess() throws IOException, InterruptedException {
        // add a person to the real provider
        Uri p = addPerson(getProvider(), ACCOUNT, "si1", "p1");
        Uri ph = Uri.withAppendedPath(p, Contacts.Photos.CONTENT_DIRECTORY);

        ContentValues values = new ContentValues();
        values.put(Photos._SYNC_DIRTY, 1);
        values.put(Photos._SYNC_VERSION, "pv1");
        values.put(Photos.LOCAL_VERSION, "pv0");
        getMockContentResolver().update(ph, values, null, null);
        // check that the photos rows look correct
        Cursor cursor = getProvider().getDatabase().query(ContactsProvider.sPhotosTable,
                null, null, null, null, null, null);
        try {
            checkNextPhoto(cursor, true, "pv0", "pv1");
            assertFalse(cursor.moveToNext());
        } finally {
            cursor.close();
        }

        values.clear();
        values.put(Photos._SYNC_DIRTY, 0);
        getMockContentResolver().update(ph, values, null, null);
        // check that the photos rows look correct
        cursor = getProvider().getDatabase().query(ContactsProvider.sPhotosTable,
                null, null, null, null, null, null);
        try {
            checkNextPhoto(cursor, false, "pv0", "pv1");
            assertFalse(cursor.moveToNext());
        } finally {
            cursor.close();
        }

        // save a downloaded photo for that person using the ContactsSyncAdapter
        byte[] remotePhotoData = "remote photo data".getBytes();
        InputStream photoStream = new ByteArrayInputStream(remotePhotoData);
        mSyncAdapter.savePhoto(ContentUris.parseId(p), photoStream, "pv1");

        // check that the photos rows look correct
        cursor = getProvider().getDatabase().query(ContactsProvider.sPhotosTable, 
                null, null, null, null, null, null);
        try {
            checkNextPhoto(cursor, false, "pv1", "pv1");
            assertFalse(cursor.moveToNext());
        } finally {
            cursor.close();
        }

        InputStream inputStream =
                Contacts.People.openContactPhotoInputStream(getMockContentResolver(), p);
        byte[] inputBytes = new byte[100];
        int totalBytesRead = 0;
        while (true) {
            int numBytesRead = inputStream.read(inputBytes, totalBytesRead, 
                    inputBytes.length - totalBytesRead);
            if (numBytesRead < 0) break;
            totalBytesRead += numBytesRead;
        }
        inputStream.close();

        assertByteArrayEquals(remotePhotoData, inputBytes, totalBytesRead);
    }

    private void assertByteArrayEquals(byte[] expected, byte[] actual, int lengthOfActual) {
        assertEquals(expected.length, lengthOfActual);
        for (int i = 0; i < expected.length; i++) assertEquals(expected[i], actual[i]);
    }

    private void checkNextPhoto(Cursor cursor, boolean expectedDirty, String expectedLocalVersion,
            String expectedServerVersion) {
        assertTrue(cursor.moveToNext());
        assertEquals(expectedDirty, getLong(cursor, Photos._SYNC_DIRTY) != 0);
        assertEquals(expectedLocalVersion, getString(cursor, Photos.LOCAL_VERSION));
        assertEquals(expectedServerVersion, getString(cursor, Photos._SYNC_VERSION));
    }

    private Uri addPerson(ContactsProvider provider, Account account,
            String name, String syncId) {
        ContentValues values = new ContentValues();
        values.put(People.NAME, name);
        values.put(People._SYNC_ACCOUNT, account.mName);
        values.put(People._SYNC_ACCOUNT_TYPE, account.mType);
        values.put(People._SYNC_ID, syncId);
        return provider.insert(People.CONTENT_URI, values);
    }

    private static GroupEntry newGroup(String title, String notes, String id, String version) {
        GroupEntry entry = new GroupEntry();
        entry.setTitle(title);
        entry.setContent(notes);
        entry.setId(ContactsSyncAdapter.getGroupsFeedForAccount(ACCOUNT) + "/" + id);
        entry.setEditUri(entry.getId() + "/" + version);
        return entry;
    }

    private static GroupEntry newDeletedGroup(String id, String version) {
        GroupEntry entry = new GroupEntry();
        entry.setDeleted(true);
        entry.setId(ContactsSyncAdapter.getGroupsFeedForAccount(ACCOUNT) + "/" + id);
        entry.setEditUri(entry.getId() + "/" + version);
        return entry;
    }

    private static void checkNextGroup(Cursor cursor, GroupEntry entry, Long syncLocalId) {
        assertTrue(cursor.moveToNext());
        assertEquals(dumpRow(cursor), entry.getId(),
                feedFromEntry(entry) + "/" + getString(cursor, SyncConstValue._SYNC_ID));
        assertEquals(dumpRow(cursor), entry.getEditUri(),
                entry.getId() + "/" + getString(cursor, SyncConstValue._SYNC_VERSION));
        assertEquals(dumpRow(cursor), ACCOUNT.mName,
                getString(cursor, SyncConstValue._SYNC_ACCOUNT));
        assertEquals(dumpRow(cursor), ACCOUNT.mType,
                getString(cursor, SyncConstValue._SYNC_ACCOUNT_TYPE));
        assertEquals(dumpRow(cursor), entry.getTitle(), getString(cursor, Groups.NAME));
        assertEquals(dumpRow(cursor), entry.getSystemGroup(), getString(cursor, Groups.SYSTEM_ID));
        assertEquals(dumpRow(cursor), entry.getContent(), getString(cursor, Groups.NOTES));
        if (syncLocalId != null) {
            assertEquals(dumpRow(cursor),
                    syncLocalId, getString(cursor, SyncConstValue._SYNC_LOCAL_ID));
        }
    }

    private static ContactEntry newPerson(String title, String notes, String id, String version) {
        ContactEntry entry = new ContactEntry();
        entry.setTitle(title);
        entry.setContent(notes);
        entry.setId(ContactsSyncAdapter.getContactsFeedForAccount(ACCOUNT) + "/" + id);
        entry.setEditUri(entry.getId() + "/" + version);
        entry.setLinkEditPhoto(ContactsSyncAdapter.getContactsFeedForAccount(ACCOUNT)
                + "/" + id + "/v1", "image/jpg");
        return entry;
    }

    private static ContactEntry newDeletedPerson(String id, String version) {
        ContactEntry entry = new ContactEntry();
        entry.setDeleted(true);
        entry.setId(ContactsSyncAdapter.getContactsFeedForAccount(ACCOUNT) + "/" + id);
        entry.setEditUri(entry.getId() + "/" + version);
        return entry;
    }

    private void checkNextCursorToEntry(Cursor cursor, Entry expectedEntry) 
            throws ParseException {
        Entry newEntry = newEmptyEntry(expectedEntry);
        assertTrue(cursor.moveToNext());
        String createUri = ContactsSyncAdapter.cursorToEntryImpl(getMockContentResolver(),
                cursor, newEntry, ACCOUNT);
        // since this is an update the createUri should be null
        assertNull(createUri);
        assertEquals(expectedEntry.getEditUri(), newEntry.getEditUri());
        if (expectedEntry instanceof ContactEntry) {
            ((ContactEntry)newEntry).setLinkEditPhoto(
                    ((ContactEntry)expectedEntry).getLinkEditPhotoHref(),
                    ((ContactEntry)expectedEntry).getLinkEditPhotoType());
        }
        final String expected = expectedEntry.toString();
        String actual = newEntry.toString();
        assertEquals("\nexpected:\n" + expected + "\nactual:\n" + actual, expected, actual);
    }

    private static void checkNextDeletedCursorToEntry(Cursor cursor, Entry expectedEntry)
            throws ParseException {
        Entry newEntry = newEmptyEntry(expectedEntry);
        assertTrue(cursor.moveToNext());
        ContactsSyncAdapter.deletedCursorToEntryImpl(cursor, newEntry, ACCOUNT);
        assertEquals(expectedEntry.getEditUri(), newEntry.getEditUri());
    }

    private static Entry newEmptyEntry(Entry expectedEntry) {
        if (expectedEntry instanceof ContactEntry) {
            return new ContactEntry();
        } else {
            return new GroupEntry();
        }
    }

    public static String feedFromEntry(Entry expectedEntry) {
        if (expectedEntry instanceof ContactEntry) {
            return ContactsSyncAdapter.getContactsFeedForAccount(ACCOUNT);
        } else {
            return ContactsSyncAdapter.getGroupsFeedForAccount(ACCOUNT);
        }
    }

    private static void checkNextPerson(ContactsProvider provider, Cursor cursor,
            ContactEntry entry, Long syncLocalId) {
        assertTrue(cursor.moveToNext());
        assertEquals(dumpRow(cursor), entry.getId(),
                feedFromEntry(entry) + "/" + getString(cursor, SyncConstValue._SYNC_ID));
        assertEquals(dumpRow(cursor), entry.getEditUri(),
                entry.getId() + "/" + getString(cursor, SyncConstValue._SYNC_VERSION));
        assertEquals(dumpRow(cursor), ACCOUNT.mName,
                getString(cursor, SyncConstValue._SYNC_ACCOUNT));
        assertEquals(dumpRow(cursor), ACCOUNT.mType,
                getString(cursor, SyncConstValue._SYNC_ACCOUNT_TYPE));
        assertEquals(dumpRow(cursor), entry.getTitle(), getString(cursor, People.NAME));
        assertEquals(dumpRow(cursor), entry.getContent(), getString(cursor, People.NOTES));
        if (syncLocalId != null) {
            assertEquals(dumpRow(cursor),
                    syncLocalId, getString(cursor, SyncConstValue._SYNC_LOCAL_ID));
        }

        Cursor groupCursor = provider.getDatabase().query(ContactsProvider.sGroupmembershipTable,
                null, Contacts.GroupMembership.PERSON_ID + "=?",
                new String[]{getString(cursor, People._ID)}, null, null, null);
        try {
            for (Object object : entry.getGroups()) {
                GroupMembershipInfo groupMembership = (GroupMembershipInfo) object;
                assertTrue(groupCursor.moveToNext());
                assertEquals(groupMembership.getGroup(),
                        ContactsSyncAdapter.getCanonicalGroupsFeedForAccount(ACCOUNT) + "/"
                                + getString(groupCursor, GroupMembership.GROUP_SYNC_ID));
            }
            assertFalse(groupCursor.moveToNext());
        } finally {
            groupCursor.close();
        }
    }

    private static void checkNextDeleted(Cursor cursor, Entry entry, Long syncLocalId) {
        assertTrue(cursor.moveToNext());
        assertEquals(dumpRow(cursor), entry.getId(),
                feedFromEntry(entry) + "/" + getString(cursor, SyncConstValue._SYNC_ID));
        assertEquals(dumpRow(cursor), entry.getEditUri(),
                entry.getId() + "/" + getString(cursor, SyncConstValue._SYNC_VERSION));
        assertEquals(dumpRow(cursor), ACCOUNT.mName,
                getString(cursor, SyncConstValue._SYNC_ACCOUNT));
        assertEquals(dumpRow(cursor), ACCOUNT.mType,
                getString(cursor, SyncConstValue._SYNC_ACCOUNT_TYPE));
        if (syncLocalId != null) {
            assertEquals(dumpRow(cursor),
                    syncLocalId.toString(), getString(cursor, SyncConstValue._SYNC_LOCAL_ID));
        }
    }


    private ContactsProvider newTemporaryProvider() {
        return (ContactsProvider)getProvider().getTemporaryInstance();
    }

    private void copyTempProviderToReal(ContactsProvider provider) {
        // copy the server diffs into the provider
        getProvider().merge(mMockSyncContext, provider, null, new SyncResult());

        // clear the _sync_id and _sync_local_id to act as if these are locally inserted
        ContentValues values = new ContentValues();
        values.put(SyncConstValue._SYNC_ID, (String)null);
        values.put(SyncConstValue._SYNC_LOCAL_ID, (String)null);
        values.put(SyncConstValue._SYNC_DIRTY, "1");
        getProvider().getDatabase().update("people", values, null, null);
    }

    private void checkCursorToEntry(ContactsProvider provider, ContactEntry entry)
            throws ParseException {
        copyTempProviderToReal(provider);
        final ContentProvider clientDiffsProvider = getClientDiffs();

        Cursor cursor = ContactsSyncAdapter.getCursorForTableImpl(clientDiffsProvider,
                entry.getClass());
        try {
            assertTrue(cursor.moveToFirst());
            assertEquals(dumpCursor(cursor), 1, cursor.getCount());
            ContactEntry newEntry = new ContactEntry();
            String editUrl = ContactsSyncAdapter.cursorToEntryImpl(getMockContentResolver(), cursor,
                    newEntry, ACCOUNT);
            entry.setId(null);
            entry.setEditUri(null);
            entry.setLinkEditPhoto(null, null);
            final String expected = entry.toString();
            final String actual = newEntry.toString();
            assertEquals("\nexpected:\n" + expected + "\nactual:\n" + actual, expected, actual);
        } finally {
            cursor.close();
        }
    }

    private ContentProvider getClientDiffs() {
        // query the provider for the client diffs
        TempProviderSyncResult result = new TempProviderSyncResult();
        getProvider().merge(mMockSyncContext, null, result, new SyncResult());
        return result.tempContentProvider;
    }

    private static long getSinglePersonId(ContactsProvider provider, String entryTitle) {
        long personId;
        Cursor cursor;
        cursor = provider.query(People.CONTENT_URI, null, null, null, null);
        try {
            assertTrue(cursor.moveToFirst());
            assertEquals(dumpCursor(cursor), 1, cursor.getCount());
            assertEquals(dumpRow(cursor), entryTitle, getString(cursor, People.NAME));
            personId = getLong(cursor, People._ID);
        } finally {
            cursor.close();
        }
        return personId;
    }

    private static String dumpRow(Cursor cursor) {
        return DatabaseUtils.dumpCurrentRowToString(cursor);
    }

    private static String dumpCursor(Cursor cursor) {
        return DatabaseUtils.dumpCursorToString(cursor);
    }

    private static void checkNextNumber(Cursor cursor, long personId, String number,
            boolean isPrimary, int type, String label) {
        assertTrue(cursor.moveToNext());
        assertEquals(dumpRow(cursor), personId, getLong(cursor, Contacts.Phones.PERSON_ID));
        assertEquals(dumpRow(cursor), number, getString(cursor, Contacts.Phones.NUMBER));
        assertEquals(dumpRow(cursor), type, getLong(cursor, Contacts.Phones.TYPE));
        assertEquals(dumpCursor(cursor), isPrimary,
                getLong(cursor, Contacts.Phones.ISPRIMARY) != 0);
        assertEquals(dumpRow(cursor), label, getString(cursor, Contacts.Phones.LABEL));
    }

    private static long getLong(Cursor cursor, String column) {
        return cursor.getLong(cursor.getColumnIndexOrThrow(column));
    }

    private static String getString(Cursor cursor, String column) {
        return cursor.getString(cursor.getColumnIndexOrThrow(column));
    }

    private static boolean isNull(Cursor cursor, String column) {
        return cursor.isNull(cursor.getColumnIndexOrThrow(column));
    }

    private static void checkNextContactMethod(Cursor cursor, long personId, String data,
            String auxData, int kind, boolean isPrimary, int type, String label) {
        assertTrue(cursor.moveToNext());
        assertEquals(dumpRow(cursor), personId, getLong(cursor, Contacts.ContactMethods.PERSON_ID));
        assertEquals(dumpRow(cursor), kind, getLong(cursor, Contacts.ContactMethods.KIND));
        assertEquals(dumpRow(cursor), data, getString(cursor, Contacts.ContactMethods.DATA));
        assertEquals(dumpRow(cursor), auxData, getString(cursor, Contacts.ContactMethods.AUX_DATA));
        assertEquals(dumpCursor(cursor), isPrimary,
                getLong(cursor, Contacts.ContactMethods.ISPRIMARY) != 0);
        assertEquals(dumpRow(cursor), type, getLong(cursor, Contacts.ContactMethods.TYPE));
        assertEquals(dumpRow(cursor), label, getString(cursor, Contacts.ContactMethods.LABEL));
    }

    private static void addPhoneNumber(ContactEntry entry, String number, boolean isPrimary,
            byte rel, String label) {
        PhoneNumber phoneNumber = new PhoneNumber();
        phoneNumber.setPhoneNumber(number);
        phoneNumber.setIsPrimary(isPrimary);
        phoneNumber.setType(rel);
        phoneNumber.setLabel(label);
        entry.addPhoneNumber(phoneNumber);
    }

    private static void addPostal(ContactEntry entry, String address, boolean isPrimary, byte rel,
            String label) {
        PostalAddress item = new PostalAddress();
        item.setValue(address);
        item.setType(rel);
        item.setLabel(label);
        item.setIsPrimary(isPrimary);
        entry.addPostalAddress(item);
    }

    private static void checkNextPostal(Cursor cursor, long personId, String address,
            boolean isPrimary, int type, String label) {
        checkNextContactMethod(cursor, personId, address, null,
                Contacts.KIND_POSTAL, isPrimary, type, label);
    }

    private static void addIm(ContactEntry entry, String address, byte protocol,
            String protocolString, boolean isPrimary, byte rel,
            String label) {
        ImAddress item = new ImAddress();
        item.setAddress(address);
        item.setProtocolPredefined(protocol);
        item.setProtocolCustom(protocolString);
        item.setLabel(label);
        item.setType(rel);
        item.setIsPrimary(isPrimary);
        entry.addImAddress(item);
    }

    private static void checkNextIm(Cursor cursor, long personId, String address,
            String auxData, boolean isPrimary, int type, String label) {
        checkNextContactMethod(cursor, personId, address, auxData,
                Contacts.KIND_IM, isPrimary, type, label);
    }

    private static void addEmail(ContactEntry entry, String address, boolean isPrimary, byte rel,
            String label) {
        EmailAddress item = new EmailAddress();
        item.setAddress(address);
        item.setIsPrimary(isPrimary);
        item.setType(rel);
        item.setLabel(label);
        entry.addEmailAddress(item);
    }

    private static void checkNextEmail(Cursor cursor, long personId, String address,
            boolean isPrimary, int type, String label) {
        checkNextContactMethod(cursor, personId, address, null,
                Contacts.KIND_EMAIL, isPrimary, type, label);
    }

    private void checkTableIsEmpty(ContactsProvider provider, Uri uri) {
        Cursor cursor;
        cursor = getProvider().query(uri, null, null, null, null);
        try {
            assertEquals(0, cursor.getCount());
        } finally {
            cursor.close();
        }
    }

    private void checkTableIsEmpty(ContactsProvider provider, String table) {
        Cursor cursor;
        cursor = getProvider().getDatabase().query(table, null, null, null, null, null, null);
        try {
            assertEquals(DatabaseUtils.dumpCursorToString(cursor), 0, cursor.getCount());
        } finally {
            cursor.close();
        }
    }

    private static void addOrganization(ContactEntry entry, String name, String title,
            boolean isPrimary, byte type, String label) {
        Organization organization = new Organization();
        organization.setName(name);
        organization.setTitle(title);
        organization.setIsPrimary(isPrimary);
        organization.setType(type);
        organization.setLabel(label);
        entry.addOrganization(organization);
    }

    private static void addExtendedProperty(ContactEntry entry, String name, String value,
            String blob) {
        ExtendedProperty extendedProperty = new ExtendedProperty();
        extendedProperty.setName(name);
        extendedProperty.setValue(value);
        extendedProperty.setXmlBlob(blob);
        entry.addExtendedProperty(extendedProperty);
    }

    private static void checkNextOrganization(Cursor cursor, long personId, String company,
            String title, boolean isPrimary, int type, String label) {
        assertTrue(cursor.moveToNext());
        assertEquals(dumpRow(cursor), personId, getLong(cursor, Contacts.Organizations.PERSON_ID));
        assertEquals(dumpRow(cursor), company, getString(cursor, Contacts.Organizations.COMPANY));
        assertEquals(dumpRow(cursor), title, getString(cursor, Contacts.Organizations.TITLE));
        assertEquals(dumpRow(cursor), isPrimary,
                getLong(cursor, Contacts.Organizations.ISPRIMARY) != 0);
        assertEquals(dumpRow(cursor), type, getLong(cursor, Contacts.Phones.TYPE));
        assertEquals(dumpRow(cursor), label, getString(cursor, Contacts.Phones.LABEL));
    }
    
    private static void checkNextExtension(Cursor cursor, long personId,
            String name, String value) {
        assertTrue(cursor.moveToNext());
        assertEquals(dumpRow(cursor), personId, getLong(cursor, Contacts.Extensions.PERSON_ID));
        assertEquals(dumpRow(cursor), name, getString(cursor, Contacts.Extensions.NAME));
        assertEquals(dumpRow(cursor), value, getString(cursor, Contacts.Extensions.VALUE));
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mSyncAdapter = (ContactsSyncAdapter)getProvider().getSyncAdapter();
        getProvider().onSyncStart(mMockSyncContext, ACCOUNT);
    }

    void checkEntries(ContactsProvider provider, boolean deleted,
            Object ... entriesAndLocalSyncIds) {
        String table;
        String sortOrder = deleted ? "_sync_id" : "name";
        final Entry firstEntry = (Entry)entriesAndLocalSyncIds[0];
        if (firstEntry instanceof GroupEntry) {
            if (deleted) {
                table = ContactsProvider.sDeletedGroupsTable;
            } else {
                table = ContactsProvider.sGroupsTable;
            }
        } else {
            if (deleted) {
                table = ContactsProvider.sDeletedPeopleTable;
            } else {
                table = ContactsProvider.sPeopleTable;
            }
        }
        Cursor cursor;
         cursor = provider.getDatabase().query(table, null, null, null, null, null, sortOrder);
         try {
             for (int i = 0; i < entriesAndLocalSyncIds.length; i += 2) {
                 Entry entry = (Entry)entriesAndLocalSyncIds[i];
                 Long syncLocalId = (Long)entriesAndLocalSyncIds[i+1];
                 if (deleted) {
                     checkNextDeleted(cursor, entry, syncLocalId);
                 } else {
                     if (firstEntry instanceof GroupEntry) {
                         checkNextGroup(cursor, (GroupEntry)entry, null);
                     } else {
                         checkNextPerson(provider, cursor, (ContactEntry)entry, null);
                     }
                 }
             }
             assertTrue(dumpCursor(cursor), cursor.isLast());
         } finally {
             cursor.close();
         }
    }
}

class MockSyncContext extends SyncContext {

    MockSyncContext() {
        super(null);
    }

    @Override
    public void setStatusText(String text) {
        Log.i("SyncTest", text);
    }
}