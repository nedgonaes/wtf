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

JNIEXPORT jobject JNICALL Java_org_wtf_client_Client_async_1open
  (JNIEnv* env, jobject obj, jstring jpath, jint jflags, jint jmode, jint jnum_replicas,
        jint jblock_size, jintArray jfd)
{
    /* make a deferred object and get c pointer to it. */
    jobject op = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct wtf_client* client = (struct wtf_client*) (*env)->GetLongField(env, obj, _client_ptr);
    struct wtf_java_client_deferred* o = (struct wtf_java_client_deferred*) (*env)->GetLongField(env, obj, _deferred_ptr);
    

    jboolean is_copy;
    o->data = (char*)(*env)->GetIntArrayElements(env, jfd, &is_copy);
    const char* path = (*env)->GetStringUTFChars(env, jpath, 0);
    o->reqid = wtf_client_open(client, path, jflags, jmode, jnum_replicas,
                    jblock_size, (int64_t*)o->data, &o->status);
    if (o->reqid < 0)
    {
        wtf_java_client_throw_exception(env, o->status, 
            wtf_client_error_message(client));
        return 0;
    }

    return op;
}

JNIEXPORT jobject JNICALL Java_org_wtf_client_Client_async_1read
  (JNIEnv* env, jobject obj, jint jfd, jbyteArray jdata, jint joffset)
{
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
    o->data = (*env)->GetByteArrayElements(env, jdata, &is_copy);
    o->data_sz = (*env)->GetArrayLength(env, jdata);
    o->jdata = jdata;
    int offset = (int)joffset;
    int fd = (int)jfd;

    o->reqid = wtf_client_read(client, fd, o->data + offset, &o->data_sz, &o->status);

    if (o->reqid < 0)
    {
        wtf_java_client_throw_exception(env, o->status, 
            wtf_client_error_message(client));
        return 0;
    }

    o->encode_return = wtf_java_client_deferred_encode_status;
    return op;
}

JNIEXPORT WTF_API void JNICALL
wtf_deferred_read_cleanup(JNIEnv* env, struct wtf_java_client_deferred* dfrd);

JNIEXPORT WTF_API void JNICALL
wtf_deferred_read_cleanup(JNIEnv* env, struct wtf_java_client_deferred* dfrd)
{
    (*env)->ReleaseByteArrayElements(env, dfrd->jdata, dfrd->data, 0);
    ERROR_CHECK_VOID();
}

JNIEXPORT jobject JNICALL Java_org_wtf_client_Client_async_1write
  (JNIEnv* env, jobject obj, jint jfd, jbyteArray jdata, jint joffset)
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
    o->data = (*env)->GetByteArrayElements(env, jdata, &is_copy);
    o->data_sz = (*env)->GetArrayLength(env, jdata);
    o->jdata = jdata;
    int offset = (int)joffset;

    o->reqid = wtf_client_write(client, jfd, o->data + offset, &o->data_sz, 
        &o->status);

    if (o->reqid < 0)
    {
        wtf_java_client_throw_exception(env, o->status, wtf_client_error_message(client));
        return 0;
    }

    o->encode_return = wtf_java_client_deferred_encode_status;
    return op;
}

JNIEXPORT WTF_API void JNICALL
wtf_deferred_write_cleanup(JNIEnv* env, struct wtf_java_client_deferred* dfrd)
{
    (*env)->ReleaseByteArrayElements(env, dfrd->jdata, dfrd->data, 0);
    ERROR_CHECK_VOID();
}
