/*
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
 */

package com.nfsdb.ql.impl.select;

import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.ops.AbstractRecordSource;
import com.nfsdb.std.CharSequenceHashSet;
import com.nfsdb.std.ObjList;

public class SelectedColumnsRecordSource extends AbstractRecordSource {
    private final RecordSource recordSource;
    private final RecordMetadata metadata;
    private final SelectedColumnsRecord record;
    private final SelectedColumnsStorageFacade storageFacade;
    private RecordCursor recordCursor;

    public SelectedColumnsRecordSource(RecordSource recordSource, ObjList<CharSequence> names, CharSequenceHashSet aliases) {
        this.recordSource = recordSource;
        RecordMetadata dm = recordSource.getMetadata();
        this.metadata = new SelectedColumnsMetadata(dm, names, aliases);
        this.record = new SelectedColumnsRecord(dm, names);
        this.storageFacade = new SelectedColumnsStorageFacade(dm, metadata, names);
    }

    public SelectedColumnsRecordSource(RecordSource recordSource, ObjList<CharSequence> names) {
        this.recordSource = recordSource;
        RecordMetadata dm = recordSource.getMetadata();
        this.metadata = new SelectedColumnsMetadata(dm, names);
        this.record = new SelectedColumnsRecord(dm, names);
        this.storageFacade = new SelectedColumnsStorageFacade(dm, metadata, names);
    }

    @Override
    public Record getByRowId(long rowId) {
        return record.of(recordCursor.getByRowId(rowId));
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.recordCursor = recordSource.prepareCursor(factory);
        this.storageFacade.of(recordCursor.getStorageFacade());
        return this;
    }

    @Override
    public void reset() {
        recordSource.reset();
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
        return record.of(recordCursor.next());
    }

    @Override
    public String toString() {
        return "SelectedColumnsRecordSource{" +
                "recordSource=" + recordSource +
                ", metadata=" + metadata +
                '}';
    }
}
