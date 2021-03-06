/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.mp;

import com.nfsdb.ex.FatalError;
import com.nfsdb.misc.Unsafe;

public abstract class SynchronizedJob implements Job {
    private static final long LOCKED_OFFSET;

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private volatile int locked = 0;

    @Override
    public boolean run() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, LOCKED_OFFSET, 0, 1)) {
            try {
                return runSerially();
            } finally {
                locked = 0;
            }
        }
        return false;
    }

    protected abstract boolean runSerially();

    static {
        try {
            LOCKED_OFFSET = Unsafe.getUnsafe().objectFieldOffset(SynchronizedJob.class.getDeclaredField("locked"));
        } catch (NoSuchFieldException e) {
            throw new FatalError(e);
        }
    }
}
