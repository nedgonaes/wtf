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


package org.wtf.client;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class Client
{
    static
    {
        System.loadLibrary("wtf-client-java");
        initialize();
    }

    private long ptr = 0;

    public Client(String wtf_host, Integer wtf_port, String hyperdex_host, Integer hyperdex_port)
    {
        _create(wtf_host, wtf_port.intValue(), hyperdex_host, hyperdex_port.intValue());
    }

    public Client(String wtf_host, int wtf_port, String hyperdex_host, int hyperdex_port)
    {
        _create(wtf_host, wtf_port, hyperdex_host, hyperdex_port);
    }

    protected void finalize() throws Throwable
    {
        try
        {
            destroy();
        }
        finally
        {
            super.finalize();
        }
    }

    public synchronized void destroy()
    {
        if (ptr != 0)
        {
            _destroy();
            ptr = 0;
        }
    }

    /* utilities */
    public Operation loop()
    {
        //XXX: rewrite
        return null;
    }

    /* cached IDs */
    private static native void initialize();
    private static native void terminate();
    /* ctor/dtor */
    private native void _create(String wtf_host, int wtf_port, String hyperdex_host, int hyperdex_port);
    private native void _destroy();
    /* utilities */
    private native long inner_loop();
    
    /* operations */
    public native Deferred async_open(String path, int flags, int mode, 
        int num_replicas, int block_size, long[] fd) throws WTFClientException;
    public Boolean open(String path, int flags, int mode, 
        int num_replicas, int block_size, long[] fd) throws WTFClientException
    {
        return (Boolean) async_open(path, flags, mode, num_replicas, block_size,
            fd).waitForIt();
    }

    public native Deferred async_read(long fd, byte[] data, int offset) throws WTFClientException;
    public Boolean read(long fd, byte[] data, int offset) throws WTFClientException
    {
        return (Boolean) async_read(fd, data, offset).waitForIt();
    }

    public native Deferred async_write(long fd, byte[] data, int offset) throws WTFClientException;
    public Boolean write(long fd, byte[] data, int offset) throws WTFClientException
    {
        return (Boolean) async_write(fd, data, offset).waitForIt();
    }

    public native Deferred async_getattr(String path, WTFFileAttrs fa) throws WTFClientException;
    public Boolean getattr(String path, WTFFileAttrs fa) throws WTFClientException
    {
        return (Boolean) async_getattr(path, fa).waitForIt();
    }

    public native Deferred async_rename(String src, String dst) throws WTFClientException;
    public Boolean rename(String src, String dst) throws WTFClientException
    {
        return (Boolean) async_rename(src, dst).waitForIt();
    }

    public native Deferred async_mkdir(String path, int permissions);
    public Boolean mkdir(String path, int permissions)
    {
        return (Boolean) async_mkdir(path, permissions).waitForIt();
    }

    public native Deferred async_unlink(String path);
    public Boolean unlink(String path)
    {
        return (Boolean) async_unlink(path).waitForIt();
    }

    public native long async_lseek(long fd, long offset, int whence);

    public native Iterator readdir(String path);

    public String error_location()
    {
        return "";
    }

    public String error_message()
    {
        return "";
    }

    public native void close(long fd);
    
};

