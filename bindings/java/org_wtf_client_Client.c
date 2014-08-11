/* Copyright (c) 2013, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of WTF nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* C */
#include <assert.h>
#include <string.h>

/* WTF */
#include <wtf.h>
#include <wtf/client.h>
#include "visibility.h"
#include "bindings/java/org_wtf_client_Client.h"
#include "bindings/java/org_wtf_client_Deferred.h"
#include "bindings/java/org_wtf_client_Iterator.h"

/********************************* Cached IDs *********************************/

static jclass _string;

static jclass _byte_string;
static jmethodID _byte_string_init;
static jmethodID _byte_string_get;

static jclass _boolean;
static jmethodID _boolean_init;

static jclass _long;
static jmethodID _long_init;
static jmethodID _long_longValue;

static jclass _integer;
static jmethodID _integer_init;
static jmethodID _integer_intValue;

static jclass _double;
static jmethodID _double_init;
static jmethodID _double_doubleValue;

static jclass _java_iterator;
static jmethodID _java_iterator_hasNext;
static jmethodID _java_iterator_next;

static jclass _wtf_except;
static jmethodID _wtf_except_init;

static jclass _deferred;
static jfieldID _deferred_c;
static jfieldID _deferred_ptr;
static jmethodID _deferred_init;
static jmethodID _deferred_loop;

static jclass _iterator;
static jfieldID _iterator_c;
static jfieldID _iterator_ptr;
static jmethodID _iterator_init;
static jmethodID _iterator_appendBacklogged;

static jclass _client;
static jfieldID _client_ptr;
static jmethodID _client_add_op;
static jmethodID _client_remove_op;

define CHECK_CACHE(X) assert((X))

#define ERROR_CHECK(RET) if ((*env)->ExceptionCheck(env) == JNI_TRUE) return (RET)
#define ERROR_CHECK_VOID() if ((*env)->ExceptionCheck(env) == JNI_TRUE) return

#define REF(NAME, DEF) \
    tmp_cls = (DEF); \
    NAME = (jclass) (*env)->NewGlobalRef(env, tmp_cls); \
    (*env)->DeleteLocalRef(env, tmp_cls);

JNIEXPORT WTF_API void JNICALL
Java_org_wtf_client_Client_initialize(JNIEnv* env, jclass client)
{
    jclass tmp_cls;

    /* cache class String */
    REF(_string, (*env)->FindClass(env, "java/lang/String"));
    /* cache class ByteString */
    REF(_byte_string, (*env)->FindClass(env, "org/wtf/client/ByteString"));
    _byte_string_init = (*env)->GetMethodID(env, _byte_string, "<init>", "([B)V");
    _byte_string_get = (*env)->GetMethodID(env, _byte_string, "getBytes", "()[B");
    /* cache class Boolean */
    REF(_boolean, (*env)->FindClass(env, "java/lang/Boolean"));
    _boolean_init = (*env)->GetMethodID(env, _boolean, "<init>", "(Z)V");
    /* cache class Integer */
    REF(_long, (*env)->FindClass(env, "java/lang/Long"));
    _long_init = (*env)->GetMethodID(env, _long, "<init>", "(J)V");
    _long_longValue = (*env)->GetMethodID(env, _long, "longValue", "()J");
    /* cache class Integer */
    REF(_integer, (*env)->FindClass(env, "java/lang/Integer"));
    _integer_init = (*env)->GetMethodID(env, _integer, "<init>", "(I)V");
    _integer_intValue = (*env)->GetMethodID(env, _integer, "intValue", "()I");
    /* cache class Double */
    REF(_double, (*env)->FindClass(env, "java/lang/Double"));
    _double_init = (*env)->GetMethodID(env, _double, "<init>", "(D)V");
    _double_doubleValue = (*env)->GetMethodID(env, _double, "doubleValue", "()D");
    /* cache class Iterator */
    REF(_java_iterator, (*env)->FindClass(env, "java/util/Iterator"));
    _java_iterator_hasNext = (*env)->GetMethodID(env, _java_iterator, "hasNext", "()Z");
    _java_iterator_next = (*env)->GetMethodID(env, _java_iterator, "next", "()Ljava/lang/Object;");
    /* cache class WTFException */
    REF(_wtf_except, (*env)->FindClass(env, "org/wtf/client/WTFClientException"));
    _wtf_except_init = (*env)->GetMethodID(env, _wtf_except, "<init>", "(JLjava/lang/String;Ljava/lang/String;)V");
    /* cache class Deferred */
    REF(_deferred, (*env)->FindClass(env, "org/wtf/client/Deferred"));
    _deferred_c = (*env)->GetFieldID(env, _deferred, "c", "Lorg/wtf/client/Client;");
    _deferred_ptr = (*env)->GetFieldID(env, _deferred, "ptr", "J");
    _deferred_init = (*env)->GetMethodID(env, _deferred, "<init>", "(Lorg/wtf/client/Client;)V");
    _deferred_loop = (*env)->GetMethodID(env, _deferred, "loop", "()V");
    /* cache class Iterator */
    REF(_iterator, (*env)->FindClass(env, "org/wtf/client/Iterator"));
    _iterator_c = (*env)->GetFieldID(env, _iterator, "c", "Lorg/wtf/client/Client;");
    _iterator_ptr = (*env)->GetFieldID(env, _iterator, "ptr", "J");
    _iterator_init = (*env)->GetMethodID(env, _iterator, "<init>", "(Lorg/wtf/client/Client;)V");
    _iterator_appendBacklogged = (*env)->GetMethodID(env, _iterator, "appendBacklogged", "(Ljava/lang/Object;)V");
    /* cache class Client */
    REF(_client, (*env)->FindClass(env, "org/wtf/client/Client"));
    _client_ptr = (*env)->GetFieldID(env, _client, "ptr", "J");
    _client_add_op = (*env)->GetMethodID(env, _client, "add_op", "(JLorg/wtf/client/Operation;)V");
    _client_remove_op = (*env)->GetMethodID(env, _client, "remove_op", "(J)V");

    CHECK_CACHE(_string);
    CHECK_CACHE(_byte_string);
    CHECK_CACHE(_byte_string_init);
    CHECK_CACHE(_byte_string_get);
    CHECK_CACHE(_boolean);
    CHECK_CACHE(_boolean_init);
    CHECK_CACHE(_long);
    CHECK_CACHE(_long_init);
    CHECK_CACHE(_long_longValue);
    CHECK_CACHE(_integer);
    CHECK_CACHE(_integer_init);
    CHECK_CACHE(_integer_intValue);
    CHECK_CACHE(_double);
    CHECK_CACHE(_double_init);
    CHECK_CACHE(_double_doubleValue);
    CHECK_CACHE(_java_iterator);
    CHECK_CACHE(_java_iterator_hasNext);
    CHECK_CACHE(_java_iterator_next);
    CHECK_CACHE(_wtf_except);
    CHECK_CACHE(_wtf_except_init);
    CHECK_CACHE(_deferred);
    CHECK_CACHE(_deferred_c);
    CHECK_CACHE(_deferred_ptr);
    CHECK_CACHE(_deferred_init);
    CHECK_CACHE(_deferred_loop);
    CHECK_CACHE(_iterator);
    CHECK_CACHE(_iterator_c);
    CHECK_CACHE(_iterator_ptr);
    CHECK_CACHE(_iterator_init);
    CHECK_CACHE(_iterator_appendBacklogged);
    CHECK_CACHE(_client);
    CHECK_CACHE(_client_ptr);
    CHECK_CACHE(_client_add_op);
    CHECK_CACHE(_client_remove_op);

    (void) client;
}

JNIEXPORT WTF_API void JNICALL
Java_org_wtf_client_Client_terminate(JNIEnv* env, jclass client)
{
    (*env)->DeleteGlobalRef(env, _string);
    (*env)->DeleteGlobalRef(env, _byte_string);
    (*env)->DeleteGlobalRef(env, _boolean);
    (*env)->DeleteGlobalRef(env, _long);
    (*env)->DeleteGlobalRef(env, _integer);
    (*env)->DeleteGlobalRef(env, _double);
    (*env)->DeleteGlobalRef(env, _wtf_except);
    (*env)->DeleteGlobalRef(env, _deferred);
    (*env)->DeleteGlobalRef(env, _iterator);
    (*env)->DeleteGlobalRef(env, _client);

    (void) client;
}

/******************************* Pointer Unwrap *******************************/

static struct wtf_client*
wtf_get_client_ptr(JNIEnv* env, jobject obj)
{
    struct wtf_client* x;
    x = (struct wtf_client*) (*env)->GetLongField(env, obj, _client_ptr);
    assert(x);
    return x;
}

static struct wtf_java_client_deferred*
wtf_get_deferred_ptr(JNIEnv* env, jobject obj)
{
    struct wtf_java_client_deferred* x;
    x = (struct wtf_java_client_deferred*) (*env)->GetLongField(env, obj, _deferred_ptr);
    assert(x);
    return x;
}

static struct wtf_java_client_iterator*
wtf_get_iterator_ptr(JNIEnv* env, jobject obj)
{
    struct wtf_java_client_iterator* x;
    x = (struct wtf_java_client_iterator*) (*env)->GetLongField(env, obj, _iterator_ptr);
    assert(x);
    return x;
}

/******************************* Error Handling *******************************/

static int
wtf_java_out_of_memory(JNIEnv* env)
{
    jclass oom;
    jmethodID init;
    jobject exc;

    oom  = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
    ERROR_CHECK(-1);
    init = (*env)->GetMethodID(env, oom, "<init>", "()V");
    ERROR_CHECK(-1);
    exc = (*env)->NewObject(env, oom, init);
    ERROR_CHECK(-1);

    (*env)->ExceptionClear(env);
    (*env)->Throw(env, exc);
    return -1;
}

static jobject
wtf_java_client_create_exception(JNIEnv* env,
                                      enum wtf_client_returncode _rc,
                                      const char* message)
{
    jlong rc = _rc;
    jstring str = (*env)->NewStringUTF(env, wtf_client_returncode_to_string(_rc));
    jstring msg = (*env)->NewStringUTF(env, message);
    jobject err = (*env)->NewObject(env, _wtf_except, _wtf_except_init, rc, str, msg);
    ERROR_CHECK(0);
    return err;
}

static int
wtf_java_client_throw_exception(JNIEnv* env,
                                     enum wtf_client_returncode _rc,
                                     const char* message)
{
    jobject err = wtf_java_client_create_exception(env, _rc, message);
    ERROR_CHECK(-1);
    (*env)->ExceptionClear(env);
    return (*env)->Throw(env, err);
}

/********************************** Java -> C *********************************/

static const char*
wtf_java_client_convert_cstring(JNIEnv* env,
                                     struct wtf_ds_arena* arena,
                                     jobject str)
{
    const char* tmp = NULL;
    const char* ret = NULL;
    size_t ret_sz = 0;
    enum wtf_ds_returncode rc;
    int success;
    tmp = (*env)->GetStringUTFChars(env, str, 0);
    ERROR_CHECK(NULL);
    success = wtf_ds_copy_string(arena, tmp, strlen(tmp) + 1, &rc, &ret, &ret_sz);
    (*env)->ReleaseStringUTFChars(env, str, tmp);
    ERROR_CHECK(NULL);

    if (success < 0)
    {
        wtf_java_out_of_memory(env);
        return NULL;
    }

    return ret;
}

typedef int (*elem_string_fptr)(void*, const char*, size_t, enum wtf_ds_returncode*);
typedef int (*elem_int_fptr)(void*, int64_t, enum wtf_ds_returncode*);
typedef int (*elem_float_fptr)(void*, double, enum wtf_ds_returncode*);

#define HDJAVA_HANDLE_ELEM_ERROR(X, TYPE) \
    switch (X) \
    { \
        case WTF_DS_NOMEM: \
            wtf_java_out_of_memory(env); \
            return -1; \
        case WTF_DS_MIXED_TYPES: \
            wtf_java_client_throw_exception(env, WTF_CLIENT_WRONGTYPE, "Cannot add " TYPE " to a heterogenous container"); \
            return -1; \
        case WTF_DS_SUCCESS: \
        case WTF_DS_STRING_TOO_LONG: \
        case WTF_DS_WRONG_STATE: \
        default: \
            wtf_java_client_throw_exception(env, WTF_CLIENT_WRONGTYPE, "Cannot convert " TYPE " to a WTF type"); \
            return -1; \
    }

static int
wtf_java_client_convert_elem(JNIEnv* env,
                                  jobject obj,
                                  void* container,
                                  elem_string_fptr f_string,
                                  elem_int_fptr f_int,
                                  elem_float_fptr f_float)
{
    enum wtf_ds_returncode error;
    int success;
    const char* tmp_str;
    size_t tmp_str_sz;
    jbyte* tmp_bytes;
    size_t tmp_bytes_sz;
    int64_t tmp_l;
    int32_t tmp_i;
    double tmp_d;

    if ((*env)->IsInstanceOf(env, obj, _string) == JNI_TRUE)
    {
        tmp_str = (*env)->GetStringUTFChars(env, obj, 0);
        ERROR_CHECK(-1);
        tmp_str_sz = (*env)->GetStringUTFLength(env, obj);
        ERROR_CHECK(-1);
        success = f_string(container, tmp_str, tmp_str_sz, &error);
        (*env)->ReleaseStringUTFChars(env, obj, tmp_str);
        ERROR_CHECK(-1);

        if (success < 0)
        {
            HDJAVA_HANDLE_ELEM_ERROR(error, "string");
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, obj, _byte_string) == JNI_TRUE)
    {
        obj = (*env)->CallObjectMethod(env, obj, _byte_string_get);
        ERROR_CHECK(-1);
        tmp_bytes = (*env)->GetByteArrayElements(env, obj, 0);
        ERROR_CHECK(-1);
        tmp_bytes_sz = (*env)->GetArrayLength(env, obj);
        ERROR_CHECK(-1);
        success = f_string(container, (const char*)tmp_bytes, tmp_bytes_sz, &error);
        (*env)->ReleaseByteArrayElements(env, obj, tmp_bytes, 0);
        ERROR_CHECK(-1);

        if (success < 0)
        {
            HDJAVA_HANDLE_ELEM_ERROR(error, "bytes");
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, obj, _long) == JNI_TRUE)
    {
        tmp_l = (*env)->CallLongMethod(env, obj, _long_longValue);
        ERROR_CHECK(-1);

        if (f_int(container, tmp_l, &error) < 0)
        {
            HDJAVA_HANDLE_ELEM_ERROR(error, "long");
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, obj, _integer) == JNI_TRUE)
    {
        tmp_i = (*env)->CallIntMethod(env, obj, _integer_intValue);
        ERROR_CHECK(-1);

        if (f_int(container, tmp_i, &error) < 0)
        {
            HDJAVA_HANDLE_ELEM_ERROR(error, "int");
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, obj, _double) == JNI_TRUE)
    {
        tmp_d = (*env)->CallDoubleMethod(env, obj, _double_doubleValue);
        ERROR_CHECK(-1);

        if (f_float(container, tmp_d, &error) < 0)
        {
            HDJAVA_HANDLE_ELEM_ERROR(error, "float");
        }

        return 0;
    }
    else
    {
        wtf_java_client_throw_exception(env, WTF_CLIENT_WRONGTYPE,
                                             "Cannot convert unknown type to a WTF type");
        return -1;
    }
}

static int
wtf_java_client_convert_type(JNIEnv* env,
                                  struct wtf_ds_arena* arena,
                                  jobject x,
                                  const char** value,
                                  size_t* value_sz,
                                  enum hyperdatatype* datatype)
{
    enum wtf_ds_returncode error;
    int success;
    const char* tmp_str;
    size_t tmp_str_sz;
    jbyte* tmp_bytes;
    size_t tmp_bytes_sz;
    int64_t tmp_l;
    int32_t tmp_i;
    double tmp_d;

    if (x == NULL)
    {
        *value = "";
        *value_sz = 0;
        *datatype = HYPERDATATYPE_GENERIC;
        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _string) == JNI_TRUE)
    {
        tmp_str = (*env)->GetStringUTFChars(env, x, 0);
        ERROR_CHECK(-1);
        tmp_str_sz = (*env)->GetStringUTFLength(env, x);
        ERROR_CHECK(-1);
        success = wtf_ds_copy_string(arena, tmp_str, tmp_str_sz,
                                          &error, value, value_sz);
        (*env)->ReleaseStringUTFChars(env, x, tmp_str);
        ERROR_CHECK(-1);
        *datatype = HYPERDATATYPE_STRING;

        if (success < 0)
        {
            wtf_java_out_of_memory(env);
            return -1;
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _byte_string) == JNI_TRUE)
    {
        x = (*env)->CallObjectMethod(env, x, _byte_string_get);
        tmp_bytes = (*env)->GetByteArrayElements(env, x, 0);
        ERROR_CHECK(-1);
        tmp_bytes_sz = (*env)->GetArrayLength(env, x);
        ERROR_CHECK(-1);
        success = wtf_ds_copy_string(arena, (const char*)tmp_bytes, tmp_bytes_sz,
                                          &error, value, value_sz);
        (*env)->ReleaseByteArrayElements(env, x, tmp_bytes, 0);
        ERROR_CHECK(-1);
        *datatype = HYPERDATATYPE_STRING;

        if (success < 0)
        {
            wtf_java_out_of_memory(env);
            return -1;
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _long) == JNI_TRUE)
    {
        tmp_l = (*env)->CallLongMethod(env, x, _long_longValue);
        ERROR_CHECK(-1);

        if (wtf_ds_copy_int(arena, tmp_l, &error, value, value_sz) < 0)
        {
            wtf_java_out_of_memory(env);
            return -1;
        }

        *datatype = HYPERDATATYPE_INT64;
        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _integer) == JNI_TRUE)
    {
        tmp_i = (*env)->CallIntMethod(env, x, _integer_intValue);
        ERROR_CHECK(-1);

        if (wtf_ds_copy_int(arena, tmp_i, &error, value, value_sz) < 0)
        {
            wtf_java_out_of_memory(env);
            return -1;
        }

        *datatype = HYPERDATATYPE_INT64;
        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _double) == JNI_TRUE)
    {
        tmp_d = (*env)->CallDoubleMethod(env, x, _double_doubleValue);
        ERROR_CHECK(-1);

        if (wtf_ds_copy_float(arena, tmp_d, &error, value, value_sz) < 0)
        {
            wtf_java_out_of_memory(env);
            return -1;
        }

        *datatype = HYPERDATATYPE_FLOAT;
        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _list) == JNI_TRUE)
    {
        return wtf_java_client_convert_list(env, arena, x,
                                                 value, value_sz, datatype);
    }
    else if ((*env)->IsInstanceOf(env, x, _set) == JNI_TRUE)
    {
        return wtf_java_client_convert_set(env, arena, x,
                                                value, value_sz, datatype);
    }
    else if ((*env)->IsInstanceOf(env, x, _map) == JNI_TRUE)
    {
        return wtf_java_client_convert_map(env, arena, x,
                                                value, value_sz, datatype);
    }
    else
    {
        wtf_java_client_throw_exception(env, WTF_CLIENT_WRONGTYPE,
                                             "Cannot convert unknown type to a WTF type");
        return -1;
    }
}

static int
wtf_java_client_convert_key(JNIEnv* env, jobject client,
                                 struct wtf_ds_arena* arena,
                                 jobject x,
                                 const char** key,
                                 size_t* key_sz)
{
    enum hyperdatatype datatype;
    (void)client;
    return wtf_java_client_convert_type(env, arena, x, key, key_sz, &datatype);
}

static int
wtf_java_client_convert_limit(JNIEnv* env, jobject client,
                                   struct wtf_ds_arena* arena,
                                   jint x,
                                   uint64_t* limit)
{
    *limit = x;
    (void)env;
    (void)client;
    (void)arena;
    return 0;
}

static int
wtf_java_client_convert_maxmin(JNIEnv* env, jobject client,
                                    struct wtf_ds_arena* arena,
                                    jboolean x,
                                    int* maxmin)
{
    *maxmin = x == JNI_TRUE ? 1 : 0;
    (void)env;
    (void)client;
    (void)arena;
    return 0;
}

/********************************** C -> Java *********************************/

#define BUILD_STRING(OUT, X, X_SZ) \
    do { \
        tmp_array = (*env)->NewByteArray(env, X_SZ); \
        ERROR_CHECK(0); \
        (*env)->SetByteArrayRegion(env, tmp_array, 0, X_SZ, (const jbyte*)X); \
        ERROR_CHECK(0); \
        OUT = (*env)->NewObject(env, _byte_string, _byte_string_init, tmp_array); \
        ERROR_CHECK(0); \
    } while (0)

#define BUILD_INT(OUT, X) \
    do { \
        OUT = (*env)->NewObject(env, _long, _long_init, X); \
        ERROR_CHECK(0); \
    } while (0)

#define BUILD_FLOAT(OUT, X) \
    do { \
        OUT = (*env)->NewObject(env, _double, _double_init, X); \
        ERROR_CHECK(0); \
    } while (0)

/******************************* Deferred Class *******************************/

struct wtf_java_client_deferred
{
    int64_t reqid;
    enum wtf_client_returncode status;
    jobject jdata;
    const char* data;
    size_t data_sz;
    const char* description;
    uint64_t count;
    int finished;
    void (*cleanup)(JNIEnv* env, struct wtf_java_client_deferred* d);
    jobject (*encode_return)(JNIEnv* env, jobject obj, struct wtf_java_client_deferred* d);
};

JNIEXPORT WTF_API void JNICALL
Java_org_wtf_client_Deferred__1create(JNIEnv* env, jobject deferred)
{
    jlong lptr;
    struct wtf_java_client_deferred* ptr;

    lptr = (*env)->GetLongField(env, deferred, _deferred_ptr);
    ERROR_CHECK_VOID();
    ptr = malloc(sizeof(struct wtf_java_client_deferred));

    if (!ptr)
    {
        wtf_java_out_of_memory(env);
        return;
    }

    memset(ptr, 0, sizeof(struct wtf_java_client_deferred));
    lptr = (long) ptr;
    (*env)->SetLongField(env, deferred, _deferred_ptr, lptr);
    ERROR_CHECK_VOID();

    ptr->reqid = -1;
    ptr->status = WTF_CLIENT_GARBAGE;
    ptr->data = NULL;
    ptr->data_sz = 0;
    ptr->description = NULL;
    ptr->count = 0;
    ptr->finished = 0;
    ptr->encode_return = NULL;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
JNIEXPORT WTF_API void JNICALL
Java_org_wtf_client_Deferred__1destroy(JNIEnv* env, jobject dfrd)
{
    jlong lptr;
    struct wtf_java_client_deferred* ptr;

    lptr = (*env)->GetLongField(env, dfrd, _deferred_ptr);
    ERROR_CHECK_VOID();
    ptr = (struct wtf_java_client_deferred*)lptr;

    if (ptr)
    {
        if (ptr->arena)
        {
            wtf_ds_arena_destroy(ptr->arena);
        }

        if (ptr->attrs)
        {
            wtf_client_destroy_attrs(ptr->attrs, ptr->attrs_sz);
        }

        if (ptr->description)
        {
            free((void*)ptr->description);
        }

        free(ptr);
    }

    (*env)->SetLongField(env, dfrd, _deferred_ptr, 0);
    ERROR_CHECK_VOID();
}
#pragma GCC diagnostic pop

JNIEXPORT WTF_API jobject JNICALL
Java_org_wtf_client_Deferred_waitForIt(JNIEnv* env, jobject obj)
{
    struct wtf_java_client_deferred* dfrd = NULL;
    dfrd = wtf_get_deferred_ptr(env, obj);
    ERROR_CHECK(0);
    
    ptr = wtf_get_client_ptr(env, client);
    x = wtf_client_loop(ptr, dfrd->reqid, -1, &rc);

    /*
     * We need to release the bytes that are pinned
     */
    assert(dfrd->cleanup);
    dfrd->cleanup(env, obj, dfrd);

    assert(dfrd->encode_return);
    return dfrd->encode_return(env, obj, dfrd);
}

JNIEXPORT WTF_API void JNICALL
Java_org_wtf_client_Deferred_callback(JNIEnv* env, jobject obj)
{
    jobject client_obj;
    struct wtf_java_client_deferred* dfrd = NULL;
    dfrd = wtf_get_deferred_ptr(env, obj);
    ERROR_CHECK_VOID();
    dfrd->finished = 1;
    client_obj = (*env)->GetObjectField(env, obj, _deferred_c);
    (*env)->CallObjectMethod(env, client_obj, _client_remove_op, dfrd->reqid);
    ERROR_CHECK_VOID();
}

static jobject
wtf_java_client_deferred_encode_status(JNIEnv* env, jobject obj, struct wtf_java_client_deferred* d)
{
    jobject ret;
    jobject client_obj;
    struct wtf_client* client;

    if (d->status == WTF_CLIENT_SUCCESS)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_TRUE);
        ERROR_CHECK(0);
        return ret;
    }
    else if (d->status == WTF_CLIENT_NOTFOUND)
    {
        return NULL;
    }
    else if (d->status == WTF_CLIENT_CMPFAIL)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_FALSE);
        ERROR_CHECK(0);
        return ret;
    }
    else
    {
        client_obj = (*env)->GetObjectField(env, obj, _deferred_c);
        ERROR_CHECK(0);
        client = wtf_get_client_ptr(env, client_obj);
        wtf_java_client_throw_exception(env, d->status, wtf_client_error_message(client));
        return 0;
    }
}

static jobject
wtf_java_client_deferred_encode_status_data(JNIEnv* env, jobject obj, struct wtf_java_client_deferred* d)
{
    jobject ret;
    jobject client_obj;
    struct wtf_client* client;

    if (d->status == WTF_CLIENT_SUCCESS)
    {
        ret = wtf_java_client_build_data(env, d->data, d->data_sz);
        ERROR_CHECK(0);
        return ret;
    }
    else if (d->status == WTF_CLIENT_NOTFOUND)
    {
        return NULL;
    }
    else if (d->status == WTF_CLIENT_CMPFAIL)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_FALSE);
        ERROR_CHECK(0);
        return ret;
    }
    else
    {
        client_obj = (*env)->GetObjectField(env, obj, _deferred_c);
        client = wtf_get_client_ptr(env, client_obj);
        wtf_java_client_throw_exception(env, d->status, wtf_client_error_message(client));
        return 0;
    }
}

/******************************* Iterator Class *******************************/

struct wtf_java_client_iterator
{
    struct wtf_ds_arena* arena;
    int64_t reqid;
    enum wtf_client_returncode status;
    const struct wtf_client_attribute* attrs;
    size_t attrs_sz;
    int finished;
    jobject (*encode_return)(JNIEnv* env, jobject obj, struct wtf_java_client_iterator* d);
};

JNIEXPORT WTF_API void JNICALL
Java_org_wtf_client_Iterator__1create(JNIEnv* env, jobject iterator)
{
    jlong lptr;
    struct wtf_java_client_iterator* ptr;

    lptr = (*env)->GetLongField(env, iterator, _iterator_ptr);
    ERROR_CHECK_VOID();
    ptr = malloc(sizeof(struct wtf_java_client_iterator));

    if (!ptr)
    {
        wtf_java_out_of_memory(env);
        return;
    }

    memset(ptr, 0, sizeof(struct wtf_java_client_iterator));
    lptr = (long) ptr;
    (*env)->SetLongField(env, iterator, _iterator_ptr, lptr);
    ERROR_CHECK_VOID();

    ptr->arena = wtf_ds_arena_create();

    if (!ptr->arena)
    {
        /* all other resources are caught by the finalizer? */
        wtf_java_out_of_memory(env);
        return;
    }

    ptr->reqid = -1;
    ptr->status = WTF_CLIENT_GARBAGE;
    ptr->attrs = NULL;
    ptr->attrs_sz = 0;
    ptr->finished = 0;
    ptr->encode_return = NULL;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
JNIEXPORT WTF_API void JNICALL
Java_org_wtf_client_Iterator__1destroy(JNIEnv* env, jobject iter)
{
    jlong lptr;
    struct wtf_java_client_iterator* ptr;

    lptr = (*env)->GetLongField(env, iter, _iterator_ptr);
    ERROR_CHECK_VOID();
    ptr = (struct wtf_java_client_iterator*)lptr;

    if (ptr)
    {
        if (ptr->arena)
        {
            wtf_ds_arena_destroy(ptr->arena);
        }

        if (ptr->attrs)
        {
            wtf_client_destroy_attrs(ptr->attrs, ptr->attrs_sz);
        }

        free(ptr);
    }

    (*env)->SetLongField(env, iter, _iterator_ptr, 0);
    ERROR_CHECK_VOID();
}

JNIEXPORT WTF_API jboolean JNICALL
Java_org_wtf_client_Iterator_finished(JNIEnv* env, jobject obj)
{
    struct wtf_java_client_iterator* iter = NULL;
    iter = wtf_get_iterator_ptr(env, obj);
    return iter->finished == 1 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT WTF_API void JNICALL
Java_org_wtf_client_Iterator_callback(JNIEnv* env, jobject obj)
{
    jobject tmp;
    jobject client_obj;
    struct wtf_client* client;
    struct wtf_java_client_iterator* iter = NULL;
    iter = wtf_get_iterator_ptr(env, obj);
    ERROR_CHECK_VOID();
    client_obj = (*env)->GetObjectField(env, obj, _iterator_c);
    ERROR_CHECK_VOID();

    if (iter->status == WTF_CLIENT_SEARCHDONE)
    {
        iter->finished = 1;
        (*env)->CallObjectMethod(env, client_obj, _client_remove_op, iter->reqid);
        ERROR_CHECK_VOID();
    }
    else if (iter->status == WTF_CLIENT_SUCCESS)
    {
        tmp = iter->encode_return(env, obj, iter);

        if (iter->attrs)
        {
            wtf_client_destroy_attrs(iter->attrs, iter->attrs_sz);
        }

        iter->attrs = NULL;
        iter->attrs_sz = 0;
        (*env)->CallObjectMethod(env, obj, _iterator_appendBacklogged, tmp);
        ERROR_CHECK_VOID();
    }
    else
    {
        client_obj = (*env)->GetObjectField(env, obj, _iterator_c);
        client = wtf_get_client_ptr(env, client_obj);
        tmp = wtf_java_client_create_exception(env, iter->status,
                                                    wtf_client_error_message(client));
        (*env)->CallObjectMethod(env, obj, _iterator_appendBacklogged, tmp);
        ERROR_CHECK_VOID();
    }
}

static jobject
wtf_java_client_iterator_encode_status_attributes(JNIEnv* env, jobject obj, struct wtf_java_client_iterator* it)
{
    jobject ret;
    jobject client_obj;
    struct wtf_client* client;

    if (it->status == WTF_CLIENT_SUCCESS)
    {
        ret = wtf_java_client_build_attributes(env, it->attrs, it->attrs_sz);
        ERROR_CHECK(0);
        return ret;
    }
    else if (it->status == WTF_CLIENT_NOTFOUND)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_TRUE);
        ERROR_CHECK(0);
        return ret;
    }
    else if (it->status == WTF_CLIENT_CMPFAIL)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_FALSE);
        ERROR_CHECK(0);
        return ret;
    }
    else
    {
        client_obj = (*env)->GetObjectField(env, obj, _deferred_c);
        client = wtf_get_client_ptr(env, client_obj);
        wtf_java_client_throw_exception(env, it->status, wtf_client_error_message(client));
        return 0;
    }
}

/******************************** Client Class ********************************/

JNIEXPORT WTF_API void JNICALL
Java_org_wtf_client_Client__1create(JNIEnv* env, jobject client, jstring _host, jint port)
{
    jlong lptr;
    const char* host;
    struct wtf_client* ptr;

    lptr = (*env)->GetLongField(env, client, _client_ptr);
    ERROR_CHECK_VOID();
    host = (*env)->GetStringUTFChars(env, _host, NULL);
    ERROR_CHECK_VOID();
    ptr = wtf_client_create(host, port);
    (*env)->ReleaseStringUTFChars(env, _host, host);

    if (!ptr)
    {
        wtf_java_out_of_memory(env);
        return;
    }

    ERROR_CHECK_VOID();
    lptr = (long) ptr;
    (*env)->SetLongField(env, client, _client_ptr, lptr);
    ERROR_CHECK_VOID();
    assert(sizeof(long) >= sizeof(struct wtf_client*));
}

JNIEXPORT WTF_API void JNICALL
Java_org_wtf_client_Client__1destroy(JNIEnv* env, jobject client)
{
    jlong lptr;
    struct wtf_client* ptr;

    lptr = (*env)->GetLongField(env, client, _client_ptr);
    ERROR_CHECK_VOID();
    ptr = (struct wtf_client*)lptr;

    if (ptr)
    {
        wtf_client_destroy(ptr);
    }

    (*env)->SetLongField(env, client, _client_ptr, 0);
    ERROR_CHECK_VOID();
}

JNIEXPORT WTF_API jlong JNICALL
Java_org_wtf_client_Client_inner_1loop(JNIEnv* env, jobject client)
{
    struct wtf_client* ptr;
    int64_t x;
    jlong y;
    enum wtf_client_returncode rc;

    ptr = wtf_get_client_ptr(env, client);
    x = wtf_client_loop(ptr, -1, &rc);

    if (x < 0)
    {
        wtf_java_client_throw_exception(env, rc, wtf_client_error_message(ptr));
        return -1;
    }

    y = (jlong)x;
    assert(x == y);
    return y;
}

#include "bindings/java/org_wtf_client_Client.definitions.c"
