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

import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.AbstractRecord;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.DirectInputStream;
import com.nfsdb.std.ObjList;

import java.io.OutputStream;

public class VirtualRecord extends AbstractRecord {
    private final int split;
    private final ObjList<VirtualColumn> virtualColumns;
    private Record base;
    private long rowNum;

    public VirtualRecord(RecordMetadata metadata, int split, ObjList<VirtualColumn> virtualColumns) {
        super(metadata);
        this.split = split;
        this.virtualColumns = virtualColumns;
    }

    @Override
    public byte get(int col) {
        return col < split ? base.get(col) : virtualColumns.get(col - split).get(this);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        if (col < split) {
            base.getBin(col, s);
        } else {
            virtualColumns.get(col - split).getBin(this, s);
        }
    }

    @Override
    public DirectInputStream getBin(int col) {
        return col < split ? base.getBin(col) : virtualColumns.get(col - split).getBin(this);
    }

    @Override
    public long getBinLen(int col) {
        return col < split ? base.getBinLen(col) : virtualColumns.get(col - split).getBinLen(this);
    }

    @Override
    public boolean getBool(int col) {
        return col < split ? base.getBool(col) : virtualColumns.get(col - split).getBool(this);
    }

    @Override
    public long getDate(int col) {
        return col < split ? base.getDate(col) : virtualColumns.get(col - split).getDate(this);
    }

    @Override
    public double getDouble(int col) {
        return col < split ? base.getDouble(col) : virtualColumns.get(col - split).getDouble(this);
    }

    @Override
    public float getFloat(int col) {
        return col < split ? base.getFloat(col) : virtualColumns.get(col - split).getFloat(this);
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return col < split ? base.getFlyweightStr(col) : virtualColumns.get(col - split).getFlyweightStr(this);
    }

    @Override
    public int getInt(int col) {
        return col < split ? base.getInt(col) : virtualColumns.get(col - split).getInt(this);
    }

    @Override
    public long getLong(int col) {
        return col < split ? base.getLong(col) : virtualColumns.get(col - split).getLong(this);
    }

    @Override
    public long getRowId() {
        return rowNum;
    }

    @Override
    public short getShort(int col) {
        return col < split ? base.getShort(col) : virtualColumns.get(col - split).getShort(this);
    }

    @Override
    public CharSequence getStr(int col) {
        return col < split ? base.getStr(col) : virtualColumns.get(col - split).getStr(this);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (col < split) {
            base.getStr(col, sink);
        } else {
            virtualColumns.get(col - split).getStr(this, sink);
        }
    }

    @Override
    public int getStrLen(int col) {
        return col < split ? base.getStrLen(col) : virtualColumns.get(col - split).getStrLen(this);
    }

    @Override
    public String getSym(int col) {
        return col < split ? base.getSym(col) : virtualColumns.get(col - split).getSym(this);
    }

    public void prepare(StorageFacade facade) {
        for (int i = 0, n = virtualColumns.size(); i < n; i++) {
            virtualColumns.getQuick(i).prepare(facade);
        }
    }

    public void setBase(Record base, long rowNum) {
        this.base = base;
        this.rowNum = rowNum;
    }
}
