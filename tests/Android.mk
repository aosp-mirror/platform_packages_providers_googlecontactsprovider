LOCAL_PATH:= $(call my-dir)
include $(CLEAR_VARS)

LOCAL_MODULE_TAGS := tests

LOCAL_SRC_FILES := $(call all-subdir-java-files)

LOCAL_JAVA_LIBRARIES := android.test.runner

LOCAL_PACKAGE_NAME := GoogleContactsProviderTests
LOCAL_CERTIFICATE := shared

LOCAL_INSTRUMENTATION_FOR := GoogleContactsProvider

include $(BUILD_PACKAGE)
