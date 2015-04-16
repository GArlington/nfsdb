/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.ql.impl;

import com.nfsdb.ql.KeyCursor;
import com.nfsdb.ql.KeySource;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.storage.SymbolTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class PartialSymbolKeySource implements KeySource, KeyCursor {

    private final String symbol;
    private final List<String> values;
    private int[] keys;
    private SymbolTable symbolTable;
    private int keyIndex;
    private int keyCount;

    public PartialSymbolKeySource(String symbol, List<String> values) {
        this.symbol = symbol;
        this.values = values;
    }

    @Override
    public KeyCursor cursor(PartitionSlice slice) {
        if (this.symbolTable == null) {
            if (this.keys == null || this.keys.length < values.size()) {
                this.keys = new int[values.size()];
            }
            this.symbolTable = slice.partition.getJournal().getSymbolTable(symbol);
            int keyCount = 0;
            for (int i = 0, k = values.size(); i < k; i++) {
                int key = symbolTable.getQuick(values.get(i));
                if (key >= 0) {
                    keys[keyCount++] = key;
                }
            }
            this.keyCount = keyCount;
        }
        this.keyIndex = 0;
        return this;
    }

    @Override
    public boolean hasNext() {
        return keyIndex < keyCount;
    }

    @Override
    public int next() {
        return keys[keyIndex++];
    }

    @Override
    public void reset() {
        symbolTable = null;
        keyIndex = 0;
    }

    @Override
    public int size() {
        return keyCount;
    }
}