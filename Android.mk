LOCAL_PATH := $(call my-dir)
include $(CLEAR_VARS)

contacts_provider_files := ../ContactsProvider/src/com/android/providers/contacts/ContactsProvider.java

LOCAL_SRC_FILES := $(call all-java-files-under,src) $(contacts_provider_files)

LOCAL_JAVA_LIBRARIES := ext

# We depend on googlelogin-client also, but that is already being included by google-framework
LOCAL_STATIC_JAVA_LIBRARIES := google-framework

LOCAL_PACKAGE_NAME := GoogleContactsProvider
LOCAL_CERTIFICATE := shared

LOCAL_OVERRIDES_PACKAGES := ContactsProvider

include $(BUILD_PACKAGE)

include $(call all-makefiles-under,$(LOCAL_PATH))
