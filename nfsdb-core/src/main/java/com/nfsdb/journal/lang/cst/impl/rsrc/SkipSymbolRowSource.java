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

package com.nfsdb.journal.lang.cst.impl.rsrc;

import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.lang.cst.PartitionSlice;
import com.nfsdb.journal.lang.cst.RowCursor;
import com.nfsdb.journal.lang.cst.RowSource;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * <p>
 * Takes stream of rowids, converts them to int values of FixedColumn and
 * returns rowids for non-repeated int values. Rowids returned on first in - first out basis.
 * </p>
 * <p>
 * One of use cases might be streaming of journal in reverse chronological order (latest rows first)
 * via this filter to receive last records for every value of given column.
 * </p>
 */
public class SkipSymbolRowSource implements RowSource, RowCursor {

    private final RowSource delegate;
    private final StringRef symbolName;
    private final TIntSet set = new TIntHashSet();
    private FixedColumn column;
    private int columnIndex = -1;
    private RowCursor cursor;
    private long rowid;

    public SkipSymbolRowSource(RowSource delegate, StringRef symbolName) {
        this.delegate = delegate;
        this.symbolName = symbolName;
    }

    @Override
    public RowCursor cursor(PartitionSlice slice) {
        if (columnIndex == -1) {
            columnIndex = slice.partition.getJournal().getMetadata().getColumnIndex(symbolName.value);
        }
        column = (FixedColumn) slice.partition.getAbstractColumn(columnIndex);
        cursor = delegate.cursor(slice);
        return this;
    }

    @Override
    public void reset() {
        columnIndex = -1;
        delegate.reset();
        set.clear();
    }

    @Override
    public boolean hasNext() {
        long rowid;
        while (cursor.hasNext()) {
            rowid = cursor.next();
            int key = column.getInt(rowid);
            if (set.add(key)) {
                this.rowid = rowid;
                return true;
            }
        }
        return false;
    }

    @Override
    public long next() {
        return rowid;
    }
}
