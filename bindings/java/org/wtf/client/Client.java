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
    private Map<Long, Operation> ops = null;

    public Client(String wtf_host, Integer wtf_port, String hyperdex_host, Integer hyperdex_port)
    {
        _create(wtf_host, wtf_port.intValue(), hyperdex_host, hyperdex_port.intValue());
        this.ops = new HashMap<Long, Operation>();
    }

    public Client(String wtf_host, int wtf_port, String hyperdex_host, int hyperdex_port)
    {
        _create(wtf_host, wtf_port, hyperdex_host, hyperdex_port);
        this.ops = new HashMap<Long, Operation>();
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
        long ret = inner_loop();
        Operation o = ops.get(ret);

        if (o != null)
        {
            o.callback();
        }

        return o;
    }

    /* cached IDs */
    private static native void initialize();
    private static native void terminate();
    /* ctor/dtor */
    private native void _create(String wtf_host, int wtf_port, String hyperdex_host, int hyperdex_port);
    private native void _destroy();
    /* utilities */
    private native long inner_loop();
    private void add_op(long l, Operation op)
    {
        ops.put(l, op);
    }
    private void remove_op(long l)
    {
        ops.remove(l);
    }
    /* operations */
    public native Deferred async_read(String path, byte[] data, int offset, int length) throws WTFClientException;
    public Object read(String path, byte[] data, int offset, int length) throws WTFClientException
    {
        return (Object) async_read(path, data, offset, length).waitForIt();
    }

    public native Deferred async_write(String path, Object data) throws WTFClientException;
    public Boolean write(String path, Object data) throws WTFClientException
    {
        return (Boolean) async_write(path, data).waitForIt();
    }
}
