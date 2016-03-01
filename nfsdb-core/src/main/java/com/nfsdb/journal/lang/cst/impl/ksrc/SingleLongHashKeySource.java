/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.lang.cst.impl.ksrc;

import com.nfsdb.journal.lang.cst.*;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;

public class SingleLongHashKeySource implements KeySource, KeyCursor {
    private final StringRef column;
    private final LongVariableSource variableSource;
    private int bucketCount = -1;
    private boolean hasNext;
    private LongVariable var;

    public SingleLongHashKeySource(StringRef column, LongVariableSource variableSource) {
        this.column = column;
        this.variableSource = variableSource;
    }

    @Override
    public KeyCursor cursor(PartitionSlice slice) {
        if (bucketCount == -1) {
            bucketCount = slice.partition.getJournal().getMetadata().getColumnMetadata(column.value).distinctCountHint;
        }
        this.hasNext = true;
        this.var = variableSource.getVariable(slice);
        return this;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public void reset() {
        bucketCount = -1;
        variableSource.reset();
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public int next() {
        hasNext = false;
        return (int) (var.getValue() % bucketCount);
    }
}
