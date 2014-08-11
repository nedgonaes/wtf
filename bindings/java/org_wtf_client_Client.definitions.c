/* Copyright (c) 2013-2014, Cornell University
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
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "visibility.h"

JNIEXPORT jobject JNICALL Java_org_wtf_client_Client_async_1read
  (JNIEnv* env, jobject obj, jstring jpath, jbyteArray jdata, jint joffset)
{
    const char* in_path;
    int success = 0;
    struct wtf_client* client = (struct wtf_client*) (*env)->GetLongField(env, obj, _client_ptr);
    jobject op = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    ERROR_CHECK(0);
    struct wtf_java_client_deferred* o = (struct wtf_java_client_deferred*) (*env)->GetLongField(env, obj, _deferred_ptr);
    ERROR_CHECK(0);

    /*
     * Get references to each piece of data, and pin it in place.
     * It remains pinned from the time we call Get<type>...() until we call
     * Release<type>...().
     *
     * We hold off on calling Release<type>...() until the deferred object's
     * waitForIt() call returns.  In order to release the java objects, we
     * need to store pointers to those parameters inside the deferred object.
     */

    jboolean is_copy; 
    const char* path = (*env)->GetStringUTFChars(env, jpath, 0);
    o->data = (*env)->GetByteArrayElements(env, jdata, &is_copy);
    o->data_sz = (*env)->GetArrayLength(env, jdata);
    o->jdata = jdata;
    int offset = (int)joffset;

    o->reqid = wtf_client_read(client, in_path, &o->status, o->data + offset, o->data_sz);

    if (o->reqid < 0)
    {
        wtf_java_client_throw_exception(env, o->status, wtf_client_error_message(client));
        return 0;
    }

    o->encode_return = wtf_java_client_deferred_encode_status_attributes;
    return op;
}

JNIEXPORT WTF_API void JNICALL
wtf_deferred_read_cleanup(JNIEnv* env, struct wtf_java_client_deferred* dfrd)
{
    (*env)->ReleaseByteArrayElements(env, dfrd->jdata, dfrd->data, 0);
    ERROR_CHECK_VOID();
}

JNIEXPORT jobject JNICALL Java_org_wtf_client_Client_async_1write
  (JNIEnv *, jobject, jstring, jobject);

JNIEXPORT WTF_API jobject JNICALL
wtf_java_client_asynccall__spacename_key_attributenames__status_attributes(JNIEnv* env, jobject obj, int64_t (*f)(struct wtf_client* client, const char* space, const char* key, size_t key_sz, const char** attrnames, size_t attrnames_sz, enum wtf_client_returncode* status, const struct wtf_client_attribute** attrs, size_t* attrs_sz), jstring spacename, jobject key, jobject attributenames)
{
    const char* in_space;
    const char* in_key;
    size_t in_key_sz;
    const char** in_attrnames;
    size_t in_attrnames_sz;
    int success = 0;
    struct wtf_client* client = wtf_get_client_ptr(env, obj);
    jobject op = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct wtf_java_client_deferred* o = NULL;
    ERROR_CHECK(0);
    o = wtf_get_deferred_ptr(env, op);
    ERROR_CHECK(0);
    success = wtf_java_client_convert_spacename(env, obj, o->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = wtf_java_client_convert_key(env, obj, o->arena, key, &in_key, &in_key_sz);
    if (success < 0) return 0;
    success = wtf_java_client_convert_attributenames(env, obj, o->arena, attributenames, &in_attrnames, &in_attrnames_sz);
    if (success < 0) return 0;
    o->reqid = f(client, in_space, in_key, in_key_sz, in_attrnames, in_attrnames_sz, &o->status, &o->attrs, &o->attrs_sz);

    if (o->reqid < 0)
    {
        wtf_java_client_throw_exception(env, o->status, wtf_client_error_message(client));
        return 0;
    }

    o->encode_return = wtf_java_client_deferred_encode_status_attributes;
    (*env)->CallObjectMethod(env, obj, _client_add_op, o->reqid, op);
    ERROR_CHECK(0);
    return op;
}

JNIEXPORT WTF_API jobject JNICALL
wtf_java_client_asynccall__spacename_key_attributes__status(JNIEnv* env, jobject obj, int64_t (*f)(struct wtf_client* client, const char* space, const char* key, size_t key_sz, const struct wtf_client_attribute* attrs, size_t attrs_sz, enum wtf_client_returncode* status), jstring spacename, jobject key, jobject attributes);

JNIEXPORT WTF_API jobject JNICALL
wtf_java_client_asynccall__spacename_key_attributes__status(JNIEnv* env, jobject obj, int64_t (*f)(struct wtf_client* client, const char* space, const char* key, size_t key_sz, const struct wtf_client_attribute* attrs, size_t attrs_sz, enum wtf_client_returncode* status), jstring spacename, jobject key, jobject attributes)
{
    const char* in_space;
    const char* in_key;
    size_t in_key_sz;
    const struct wtf_client_attribute* in_attrs;
    size_t in_attrs_sz;
    int success = 0;
    struct wtf_client* client = wtf_get_client_ptr(env, obj);
    jobject op = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct wtf_java_client_deferred* o = NULL;
    ERROR_CHECK(0);
    o = wtf_get_deferred_ptr(env, op);
    ERROR_CHECK(0);
    success = wtf_java_client_convert_spacename(env, obj, o->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = wtf_java_client_convert_key(env, obj, o->arena, key, &in_key, &in_key_sz);
    if (success < 0) return 0;
    success = wtf_java_client_convert_attributes(env, obj, o->arena, attributes, &in_attrs, &in_attrs_sz);
    if (success < 0) return 0;
    o->reqid = f(client, in_space, in_key, in_key_sz, in_attrs, in_attrs_sz, &o->status);

    if (o->reqid < 0)
    {
        wtf_java_client_throw_exception(env, o->status, wtf_client_error_message(client));
        return 0;
    }

    o->encode_return = wtf_java_client_deferred_encode_status;
    (*env)->CallObjectMethod(env, obj, _client_add_op, o->reqid, op);
    ERROR_CHECK(0);
    return op;
}

JNIEXPORT WTF_API jobject JNICALL
Java_org_wtf_client_Client_async_1count(JNIEnv* env, jobject obj, jstring spacename, jobject predicates)
{
    return wtf_java_client_asynccall__spacename_predicates__status_count(env, obj, wtf_client_count, spacename, predicates);
}
