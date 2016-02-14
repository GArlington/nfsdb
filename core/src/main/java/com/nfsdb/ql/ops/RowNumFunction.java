/*******************************************************************************
 * _  _ ___ ___     _ _
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
package com.nfsdb.ql.ops;

import com.nfsdb.ex.ParserException;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.virtual.VirtualRecord;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;

public class RowNumFunction extends AbstractVirtualColumn implements Function {
    public static final RowNumFunction FACTORY = new RowNumFunction();

    protected RowNumFunction() {
        super(ColumnType.LONG);
    }

    @Override
    public long getLong(Record rec) {
        assert rec instanceof VirtualRecord;
        return rec.getRowId();
    }

    @Override
    public void checkUsage(int position, ColumnUsage usage) throws ParserException {
        if (usage != ColumnUsage.SELECT) {
            throw new ParserException(position, "Function ROW_NUMBER() cannot be used in " + usage);
        }
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void prepare(StorageFacade facade) {
    }

    @Override
    public Function newInstance(ObjList<VirtualColumn> args) {
        return this;
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        throw new UnsupportedOperationException();
    }
}
