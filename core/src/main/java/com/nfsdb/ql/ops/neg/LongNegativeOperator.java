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

package com.nfsdb.ql.ops.neg;

import com.nfsdb.ex.ParserException;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.ops.AbstractVirtualColumn;
import com.nfsdb.ql.ops.Function;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.ObjectFactory;
import com.nfsdb.store.ColumnType;

public class LongNegativeOperator extends AbstractVirtualColumn implements Function {

    public final static ObjectFactory<Function> FACTORY = new ObjectFactory<Function>() {
        @Override
        public Function newInstance() {
            return new LongNegativeOperator();
        }
    };

    private VirtualColumn value;

    private LongNegativeOperator() {
        super(ColumnType.LONG);
    }

    @Override
    public double getDouble(Record rec) {
        long l = value.getLong(rec);
        return l > Long.MIN_VALUE ? -l : Double.NaN;
    }

    @Override
    public long getLong(Record rec) {
        long l = value.getLong(rec);
        return l > Long.MIN_VALUE ? -l : l;
    }

    @Override
    public boolean isConstant() {
        return value.isConstant();
    }

    @Override
    public void prepare(StorageFacade facade) {

    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        value = arg;
    }
}
