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

package com.nfsdb.ql.impl.virtual;

import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.AbstractImmutableIterator;
import com.nfsdb.std.ObjList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class VirtualColumnRecordSource extends AbstractImmutableIterator<Record> implements RecordSource<Record>, RecordCursor<Record> {
    private final RecordSource<? extends Record> recordSource;
    private final RecordMetadata metadata;
    private final VirtualRecord current;
    private RecordCursor<? extends Record> recordCursor;
    private long rowNum;


    public VirtualColumnRecordSource(RecordSource<? extends Record> recordSource, ObjList<VirtualColumn> virtualColumns) {
        this.recordSource = recordSource;
        RecordMetadata dm = recordSource.getMetadata();
        this.metadata = new VirtualRecordMetadata(dm, virtualColumns);
        this.current = new VirtualRecord(this.metadata, dm.getColumnCount(), virtualColumns);
    }

    @Override
    public Record getByRowId(long rowId) {
        current.setBase(recordCursor.getByRowId(rowId), rowNum);
        return current;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return recordCursor.getStorageFacade();
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.recordCursor = recordSource.prepareCursor(factory);
        current.prepare(recordCursor.getStorageFacade());
        return this;
    }

    @Override
    public void reset() {
        recordSource.reset();
        rowNum = 0;
    }

    @Override
    public boolean supportsRowIdAccess() {
        return recordSource.supportsRowIdAccess();
    }

    @Override
    public boolean hasNext() {
        return recordCursor.hasNext();
    }

    @Override
    public Record next() {
        ++rowNum;
        current.setBase(recordCursor.next(), rowNum);
        return current;
    }
}
