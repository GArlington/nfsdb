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

package com.nfsdb.ql.ops;

import com.nfsdb.misc.Chars;
import com.nfsdb.ql.ops.conv.*;
import com.nfsdb.ql.ops.count.*;
import com.nfsdb.ql.ops.div.DivDoubleOperator;
import com.nfsdb.ql.ops.eq.*;
import com.nfsdb.ql.ops.first.FirstDoubleAggregator;
import com.nfsdb.ql.ops.first.FirstFloatAggregator;
import com.nfsdb.ql.ops.first.FirstIntAggregator;
import com.nfsdb.ql.ops.first.FirstLongAggregator;
import com.nfsdb.ql.ops.gt.DoubleGreaterThanOperator;
import com.nfsdb.ql.ops.gt.IntGreaterThanOperator;
import com.nfsdb.ql.ops.gt.LongGreaterThanOperator;
import com.nfsdb.ql.ops.gte.DoubleGreaterOrEqualOperator;
import com.nfsdb.ql.ops.gte.IntGreaterOrEqualOperator;
import com.nfsdb.ql.ops.gte.LongGreaterOrEqualOperator;
import com.nfsdb.ql.ops.last.*;
import com.nfsdb.ql.ops.lt.DoubleLessThanOperator;
import com.nfsdb.ql.ops.lt.IntLessThanOperator;
import com.nfsdb.ql.ops.lt.LongLessThanOperator;
import com.nfsdb.ql.ops.lte.DoubleLessOrEqualOperator;
import com.nfsdb.ql.ops.lte.IntLessOrEqualOperator;
import com.nfsdb.ql.ops.lte.LongLessOrEqualOperator;
import com.nfsdb.ql.ops.max.MaxDateAggregator;
import com.nfsdb.ql.ops.max.MaxDoubleAggregator;
import com.nfsdb.ql.ops.max.MaxIntAggregator;
import com.nfsdb.ql.ops.max.MaxLongAggregator;
import com.nfsdb.ql.ops.min.MinDateAggregator;
import com.nfsdb.ql.ops.min.MinDoubleAggregator;
import com.nfsdb.ql.ops.min.MinIntAggregator;
import com.nfsdb.ql.ops.min.MinLongAggregator;
import com.nfsdb.ql.ops.minus.MinusDoubleOperator;
import com.nfsdb.ql.ops.minus.MinusIntOperator;
import com.nfsdb.ql.ops.minus.MinusLongOperator;
import com.nfsdb.ql.ops.mult.MultDoubleOperator;
import com.nfsdb.ql.ops.mult.MultIntOperator;
import com.nfsdb.ql.ops.mult.MultLongOperator;
import com.nfsdb.ql.ops.neg.DoubleNegativeOperator;
import com.nfsdb.ql.ops.neg.IntNegativeOperator;
import com.nfsdb.ql.ops.neg.LongNegativeOperator;
import com.nfsdb.ql.ops.neq.*;
import com.nfsdb.ql.ops.plus.AddDoubleOperator;
import com.nfsdb.ql.ops.plus.AddIntOperator;
import com.nfsdb.ql.ops.plus.AddLongOperator;
import com.nfsdb.ql.ops.plus.StrConcatOperator;
import com.nfsdb.ql.ops.sum.SumDoubleAggregator;
import com.nfsdb.ql.ops.sum.SumIntAggregator;
import com.nfsdb.ql.ops.sum.SumLongAggregator;
import com.nfsdb.std.CharSequenceHashSet;
import com.nfsdb.std.ObjList;
import com.nfsdb.std.ObjObjHashMap;
import com.nfsdb.store.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class FunctionFactories {

    private static final ObjObjHashMap<Signature, FunctionFactory> factories = new ObjObjHashMap<>();
    private static final CharSequenceHashSet aggregateFunctionNames = new CharSequenceHashSet();

    private FunctionFactories() {
    }

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public static FunctionFactory find(Signature sig, ObjList<VirtualColumn> args) {
        if (Chars.equals("=", sig.name) &&
                sig.paramCount == 2 &&
                sig.paramTypes.getQuick(1) == ColumnType.DOUBLE &&
                args.getQuick(1).isConstant()) {
            double d = args.getQuick(1).getDouble(null);

            // NaN
            if (d != d) {
                switch (sig.paramTypes.getQuick(0)) {
                    case DOUBLE:
                        return DoubleEqualsNanOperator.FACTORY;
                    case INT:
                        return IntEqualsNaNOperator.FACTORY;
                    case LONG:
                        return LongEqualsNaNOperator.FACTORY;
                }
            }
        }

        if (Chars.equals("!=", sig.name) &&
                sig.paramCount == 2 &&
                sig.paramTypes.getQuick(1) == ColumnType.DOUBLE &&
                args.getQuick(1).isConstant()) {
            double d = args.getQuick(1).getDouble(null);

            // NaN
            if (d != d) {
                switch (sig.paramTypes.getQuick(0)) {
                    case DOUBLE:
                        return DoubleNotEqualsNanOperator.FACTORY;
                    case INT:
                        return IntNotEqualsNaNOperator.FACTORY;
                    case LONG:
                        return LongNotEqualsNaNOperator.FACTORY;
                }
            }
        }

        FunctionFactory factory = factories.get(sig);
        if (factory != null) {
            return factory;
        } else {
            // special cases/intrinsic factories
            if (Chars.equals("in", sig.name)) {
                switch (sig.paramTypes.getQuick(0)) {
                    case STRING:
                        return StrInOperator.FACTORY;
                    case SYMBOL:
                        return SymInOperator.FACTORY;
                }
            }
        }
        return null;
    }

    public static boolean isAggregate(CharSequence name) {
        return aggregateFunctionNames.contains(name);
    }

    private static void binSig(String name, ColumnType lhst, ColumnType rhst, FunctionFactory f) {
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, false).paramType(1, rhst, false), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, true).paramType(1, rhst, false), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, false).paramType(1, rhst, true), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, true).paramType(1, rhst, true), f);
    }

    private static void virUn(String name, ColumnType type, FunctionFactory f) {
        factories.put(new Signature().setName(name).setParamCount(0), f);
    }

    private static void unSig(String name, ColumnType type, FunctionFactory f) {
        factories.put(new Signature().setName(name).setParamCount(1).paramType(0, type, false), f);
        factories.put(new Signature().setName(name).setParamCount(1).paramType(0, type, true), f);
    }

    private static void noargSig(String name, FunctionFactory f) {
        factories.put(new Signature().setName(name).setParamCount(0), f);
    }

    private static void noargSigAgg(String name, FunctionFactory f) {
        noargSig(name, f);
        aggregateFunctionNames.add(name);
    }

    private static void unSigAgg(String name, ColumnType type, FunctionFactory f) {
        unSig(name, type, f);
        aggregateFunctionNames.add(name);
    }

    private static void binSigAgg(String name, ColumnType lhst, ColumnType rhst, FunctionFactory f) {
        binSig(name, lhst, rhst, f);
        aggregateFunctionNames.add(name);
    }

    private static void triSig(String name, ColumnType lhst, ColumnType rhst, ColumnType scale, FunctionFactory f) {
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, false).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, false).paramType(2, scale, true), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, true).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, true).paramType(2, scale, true), f);

        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, false).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, false).paramType(2, scale, true), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, true).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, true).paramType(2, scale, true), f);
    }

    static {
        binSig("+", ColumnType.DOUBLE, ColumnType.DOUBLE, AddDoubleOperator.FACTORY);
        binSig("+", ColumnType.DOUBLE, ColumnType.LONG, AddDoubleOperator.FACTORY);
        binSig("+", ColumnType.LONG, ColumnType.DOUBLE, AddDoubleOperator.FACTORY);
        binSig("+", ColumnType.DOUBLE, ColumnType.INT, AddDoubleOperator.FACTORY);
        binSig("+", ColumnType.INT, ColumnType.DOUBLE, AddDoubleOperator.FACTORY);
        binSig("+", ColumnType.INT, ColumnType.INT, AddIntOperator.FACTORY);
        binSig("+", ColumnType.LONG, ColumnType.INT, AddLongOperator.FACTORY);
        binSig("+", ColumnType.INT, ColumnType.LONG, AddLongOperator.FACTORY);
        binSig("+", ColumnType.LONG, ColumnType.LONG, AddLongOperator.FACTORY);
        binSig("+", ColumnType.STRING, ColumnType.STRING, StrConcatOperator.FACTORY);

        binSig("/", ColumnType.DOUBLE, ColumnType.DOUBLE, DivDoubleOperator.FACTORY);
        binSig("/", ColumnType.DOUBLE, ColumnType.INT, DivDoubleOperator.FACTORY);
        binSig("/", ColumnType.INT, ColumnType.DOUBLE, DivDoubleOperator.FACTORY);
        binSig("/", ColumnType.INT, ColumnType.INT, DivDoubleOperator.FACTORY);
        binSig("/", ColumnType.DOUBLE, ColumnType.LONG, DivDoubleOperator.FACTORY);
        binSig("/", ColumnType.LONG, ColumnType.DOUBLE, DivDoubleOperator.FACTORY);
        binSig("/", ColumnType.LONG, ColumnType.LONG, DivDoubleOperator.FACTORY);
        binSig("/", ColumnType.LONG, ColumnType.INT, DivDoubleOperator.FACTORY);
        binSig("/", ColumnType.INT, ColumnType.LONG, DivDoubleOperator.FACTORY);

        binSig("*", ColumnType.DOUBLE, ColumnType.DOUBLE, MultDoubleOperator.FACTORY);
        binSig("*", ColumnType.INT, ColumnType.DOUBLE, MultDoubleOperator.FACTORY);
        binSig("*", ColumnType.DOUBLE, ColumnType.INT, MultDoubleOperator.FACTORY);
        binSig("*", ColumnType.DOUBLE, ColumnType.LONG, MultDoubleOperator.FACTORY);
        binSig("*", ColumnType.LONG, ColumnType.DOUBLE, MultDoubleOperator.FACTORY);
        binSig("*", ColumnType.INT, ColumnType.INT, MultIntOperator.FACTORY);
        binSig("*", ColumnType.LONG, ColumnType.LONG, MultLongOperator.FACTORY);
        binSig("*", ColumnType.INT, ColumnType.LONG, MultLongOperator.FACTORY);
        binSig("*", ColumnType.LONG, ColumnType.INT, MultLongOperator.FACTORY);

        binSig("-", ColumnType.DOUBLE, ColumnType.DOUBLE, MinusDoubleOperator.FACTORY);
        binSig("-", ColumnType.INT, ColumnType.DOUBLE, MinusDoubleOperator.FACTORY);
        binSig("-", ColumnType.DOUBLE, ColumnType.INT, MinusDoubleOperator.FACTORY);
        binSig("-", ColumnType.DOUBLE, ColumnType.LONG, MinusDoubleOperator.FACTORY);
        binSig("-", ColumnType.LONG, ColumnType.DOUBLE, MinusDoubleOperator.FACTORY);
        binSig("-", ColumnType.INT, ColumnType.INT, MinusIntOperator.FACTORY);
        binSig("-", ColumnType.LONG, ColumnType.LONG, MinusLongOperator.FACTORY);
        binSig("-", ColumnType.LONG, ColumnType.INT, MinusLongOperator.FACTORY);
        binSig("-", ColumnType.INT, ColumnType.LONG, MinusLongOperator.FACTORY);

        binSig(">", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleGreaterThanOperator.FACTORY);
        binSig(">", ColumnType.INT, ColumnType.DOUBLE, DoubleGreaterThanOperator.FACTORY);
        binSig(">", ColumnType.DOUBLE, ColumnType.INT, DoubleGreaterThanOperator.FACTORY);
        binSig(">", ColumnType.INT, ColumnType.INT, IntGreaterThanOperator.FACTORY);
        binSig(">", ColumnType.DOUBLE, ColumnType.LONG, DoubleGreaterThanOperator.FACTORY);
        binSig(">", ColumnType.LONG, ColumnType.DOUBLE, DoubleGreaterThanOperator.FACTORY);
        binSig(">", ColumnType.LONG, ColumnType.LONG, LongGreaterThanOperator.FACTORY);
        binSig(">", ColumnType.INT, ColumnType.LONG, LongGreaterThanOperator.FACTORY);
        binSig(">", ColumnType.LONG, ColumnType.INT, LongGreaterThanOperator.FACTORY);

        binSig(">=", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleGreaterOrEqualOperator.FACTORY);
        binSig(">=", ColumnType.INT, ColumnType.DOUBLE, DoubleGreaterOrEqualOperator.FACTORY);
        binSig(">=", ColumnType.DOUBLE, ColumnType.INT, DoubleGreaterOrEqualOperator.FACTORY);
        binSig(">=", ColumnType.DOUBLE, ColumnType.LONG, DoubleGreaterOrEqualOperator.FACTORY);
        binSig(">=", ColumnType.LONG, ColumnType.DOUBLE, DoubleGreaterOrEqualOperator.FACTORY);
        binSig(">=", ColumnType.INT, ColumnType.INT, IntGreaterOrEqualOperator.FACTORY);
        binSig(">=", ColumnType.LONG, ColumnType.LONG, LongGreaterOrEqualOperator.FACTORY);
        binSig(">=", ColumnType.LONG, ColumnType.INT, LongGreaterOrEqualOperator.FACTORY);
        binSig(">=", ColumnType.INT, ColumnType.LONG, LongGreaterOrEqualOperator.FACTORY);

        binSig("<", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleLessThanOperator.FACTORY);
        binSig("<", ColumnType.INT, ColumnType.DOUBLE, DoubleLessThanOperator.FACTORY);
        binSig("<", ColumnType.DOUBLE, ColumnType.INT, DoubleLessThanOperator.FACTORY);
        binSig("<", ColumnType.DOUBLE, ColumnType.LONG, DoubleLessThanOperator.FACTORY);
        binSig("<", ColumnType.LONG, ColumnType.DOUBLE, DoubleLessThanOperator.FACTORY);
        binSig("<", ColumnType.INT, ColumnType.INT, IntLessThanOperator.FACTORY);
        binSig("<", ColumnType.LONG, ColumnType.LONG, LongLessThanOperator.FACTORY);
        binSig("<", ColumnType.LONG, ColumnType.INT, LongLessThanOperator.FACTORY);
        binSig("<", ColumnType.INT, ColumnType.LONG, LongLessThanOperator.FACTORY);

        binSig("<=", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleLessOrEqualOperator.FACTORY);
        binSig("<=", ColumnType.INT, ColumnType.DOUBLE, DoubleLessOrEqualOperator.FACTORY);
        binSig("<=", ColumnType.DOUBLE, ColumnType.INT, DoubleLessOrEqualOperator.FACTORY);
        binSig("<=", ColumnType.DOUBLE, ColumnType.LONG, DoubleLessOrEqualOperator.FACTORY);
        binSig("<=", ColumnType.LONG, ColumnType.DOUBLE, DoubleLessOrEqualOperator.FACTORY);
        binSig("<=", ColumnType.INT, ColumnType.INT, IntLessOrEqualOperator.FACTORY);
        binSig("<=", ColumnType.LONG, ColumnType.LONG, LongLessOrEqualOperator.FACTORY);
        binSig("<=", ColumnType.LONG, ColumnType.INT, LongLessOrEqualOperator.FACTORY);
        binSig("<=", ColumnType.INT, ColumnType.LONG, LongLessOrEqualOperator.FACTORY);

        binSig("=", ColumnType.INT, ColumnType.INT, IntEqualsOperator.FACTORY);
        binSig("=", ColumnType.STRING, ColumnType.STRING, StrEqualsOperator.FACTORY);
        binSig("=", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleEqualsOperator.FACTORY);
        binSig("=", ColumnType.INT, ColumnType.DOUBLE, DoubleEqualsOperator.FACTORY);
        binSig("=", ColumnType.DOUBLE, ColumnType.INT, DoubleEqualsOperator.FACTORY);
        binSig("=", ColumnType.DOUBLE, ColumnType.LONG, DoubleEqualsOperator.FACTORY);
        binSig("=", ColumnType.LONG, ColumnType.DOUBLE, DoubleEqualsOperator.FACTORY);
        binSig("=", ColumnType.LONG, ColumnType.LONG, LongEqualsOperator.FACTORY);
        binSig("=", ColumnType.LONG, ColumnType.INT, LongEqualsOperator.FACTORY);
        binSig("=", ColumnType.INT, ColumnType.LONG, LongEqualsOperator.FACTORY);
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, false), StrEqualsOperator.FACTORY);
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), SymEqualsOperator.FACTORY);
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.SYMBOL, false), SymEqualsROperator.FACTORY);

        binSig("!=", ColumnType.INT, ColumnType.INT, IntNotEqualsOperator.FACTORY);
        binSig("!=", ColumnType.STRING, ColumnType.STRING, StrNotEqualsOperator.FACTORY);
        binSig("!=", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleNotEqualsOperator.FACTORY);
        binSig("!=", ColumnType.INT, ColumnType.DOUBLE, DoubleNotEqualsOperator.FACTORY);
        binSig("!=", ColumnType.DOUBLE, ColumnType.INT, DoubleNotEqualsOperator.FACTORY);
        binSig("!=", ColumnType.DOUBLE, ColumnType.LONG, DoubleNotEqualsOperator.FACTORY);
        binSig("!=", ColumnType.LONG, ColumnType.DOUBLE, DoubleNotEqualsOperator.FACTORY);
        binSig("!=", ColumnType.LONG, ColumnType.LONG, LongNotEqualsOperator.FACTORY);
        binSig("!=", ColumnType.LONG, ColumnType.INT, LongNotEqualsOperator.FACTORY);
        binSig("!=", ColumnType.INT, ColumnType.LONG, LongNotEqualsOperator.FACTORY);
        factories.put(new Signature().setName("!=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, false), StrNotEqualsOperator.FACTORY);
        factories.put(new Signature().setName("!=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), SymNotEqualsOperator.FACTORY);
        factories.put(new Signature().setName("!=").setParamCount(2).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.SYMBOL, false), SymNotEqualsROperator.FACTORY);

        triSig("eq", ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.INT, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.INT, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.LONG, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.LONG, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);

        unSig("-", ColumnType.INT, IntNegativeOperator.FACTORY);
        unSig("-", ColumnType.DOUBLE, DoubleNegativeOperator.FACTORY);
        unSig("-", ColumnType.LONG, LongNegativeOperator.FACTORY);

        unSig("not", ColumnType.BOOLEAN, NotOperator.FACTORY);
        unSig("_stoa", ColumnType.SYMBOL, StoAFunction.FACTORY);
        unSig("_atos", ColumnType.STRING, AtoSFunction.FACTORY);
        unSig("atoi", ColumnType.STRING, AtoIFunction.FACTORY);
        unSig("ltod", ColumnType.LONG, LtoDFunction.FACTORY);
        unSig("dtol", ColumnType.DATE, DtoLFunction.FACTORY);

        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.STRING, false).paramType(1, ColumnType.STRING, true), StrRegexOperator.FACTORY);
        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), SymRegexOperator.FACTORY);
        binSig("and", ColumnType.BOOLEAN, ColumnType.BOOLEAN, AndOperator.FACTORY);
        binSig("or", ColumnType.BOOLEAN, ColumnType.BOOLEAN, OrOperator.FACTORY);

        // aggregators
        unSigAgg("sum", ColumnType.DOUBLE, SumDoubleAggregator.FACTORY);
        unSigAgg("sum", ColumnType.FLOAT, SumDoubleAggregator.FACTORY);
        unSigAgg("sum", ColumnType.INT, SumIntAggregator.FACTORY);
        unSigAgg("lsum", ColumnType.INT, SumLongAggregator.FACTORY);
        unSigAgg("sum", ColumnType.LONG, SumLongAggregator.FACTORY);
        unSigAgg("sum", ColumnType.DATE, SumLongAggregator.FACTORY);

        unSigAgg("first", ColumnType.DOUBLE, FirstDoubleAggregator.FACTORY);
        unSigAgg("first", ColumnType.FLOAT, FirstFloatAggregator.FACTORY);
        unSigAgg("first", ColumnType.INT, FirstIntAggregator.FACTORY);
        unSigAgg("first", ColumnType.LONG, FirstLongAggregator.FACTORY);
        unSigAgg("first", ColumnType.DATE, FirstLongAggregator.FACTORY);

        unSigAgg("last", ColumnType.DOUBLE, LastDoubleAggregator.FACTORY);
        unSigAgg("last", ColumnType.FLOAT, LastFloatAggregator.FACTORY);
        unSigAgg("last", ColumnType.INT, LastIntAggregator.FACTORY);
        unSigAgg("last", ColumnType.LONG, LastLongAggregator.FACTORY);
        unSigAgg("last", ColumnType.DATE, LastDateAggregator.FACTORY);

        unSigAgg("avg", ColumnType.DOUBLE, AvgAggregator.FACTORY);
        unSigAgg("avg", ColumnType.INT, AvgAggregator.FACTORY);
        unSigAgg("avg", ColumnType.LONG, AvgAggregator.FACTORY);
        unSigAgg("avg", ColumnType.FLOAT, AvgAggregator.FACTORY);

        binSigAgg("vwap", ColumnType.DOUBLE, ColumnType.DOUBLE, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.DOUBLE, ColumnType.FLOAT, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.DOUBLE, ColumnType.INT, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.DOUBLE, ColumnType.LONG, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.FLOAT, ColumnType.DOUBLE, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.FLOAT, ColumnType.FLOAT, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.FLOAT, ColumnType.INT, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.FLOAT, ColumnType.LONG, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.INT, ColumnType.DOUBLE, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.INT, ColumnType.FLOAT, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.INT, ColumnType.INT, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.INT, ColumnType.LONG, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.LONG, ColumnType.DOUBLE, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.LONG, ColumnType.FLOAT, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.LONG, ColumnType.INT, VwapAggregator.FACTORY);
        binSigAgg("vwap", ColumnType.LONG, ColumnType.LONG, VwapAggregator.FACTORY);

        unSigAgg("min", ColumnType.DOUBLE, MinDoubleAggregator.FACTORY);
        unSigAgg("min", ColumnType.FLOAT, MinDoubleAggregator.FACTORY);
        unSigAgg("min", ColumnType.INT, MinIntAggregator.FACTORY);
        unSigAgg("min", ColumnType.LONG, MinLongAggregator.FACTORY);
        unSigAgg("min", ColumnType.DATE, MinDateAggregator.FACTORY);

        unSigAgg("max", ColumnType.DOUBLE, MaxDoubleAggregator.FACTORY);
        unSigAgg("max", ColumnType.FLOAT, MaxDoubleAggregator.FACTORY);
        unSigAgg("max", ColumnType.INT, MaxIntAggregator.FACTORY);
        unSigAgg("max", ColumnType.LONG, MaxLongAggregator.FACTORY);
        unSigAgg("max", ColumnType.DATE, MaxDateAggregator.FACTORY);

        noargSigAgg("count", CountAggregator.FACTORY);
        unSigAgg("count", ColumnType.DOUBLE, CountDoubleAggregator.FACTORY);
        unSigAgg("count", ColumnType.FLOAT, CountFloatAggregator.FACTORY);
        unSigAgg("count", ColumnType.INT, CountIntAggregator.FACTORY);
        unSigAgg("count", ColumnType.LONG, CountLongAggregator.FACTORY);
        unSigAgg("count", ColumnType.DATE, CountLongAggregator.FACTORY);
        unSigAgg("count", ColumnType.STRING, CountStrAggregator.FACTORY);
        unSigAgg("count", ColumnType.SYMBOL, CountSymAggregator.FACTORY);

        // virtual columns
        virUn("row_number", ColumnType.LONG, RowNumFunction.FACTORY);
    }
}
