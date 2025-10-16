/*
 * Copyright 2023 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.es8.dao.query.parser;

import java.io.InputStream;
import java.util.List;

import com.netflix.conductor.es8.dao.query.parser.internal.AbstractNode;
import com.netflix.conductor.es8.dao.query.parser.internal.ComparisonOp;
import com.netflix.conductor.es8.dao.query.parser.internal.ComparisonOp.Operators;
import com.netflix.conductor.es8.dao.query.parser.internal.ConstValue;
import com.netflix.conductor.es8.dao.query.parser.internal.ListConst;
import com.netflix.conductor.es8.dao.query.parser.internal.Name;
import com.netflix.conductor.es8.dao.query.parser.internal.ParserException;
import com.netflix.conductor.es8.dao.query.parser.internal.Range;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;

/**
 * @author Viren
 *     <pre>
 * Represents an expression of the form as below:
 * key OPR value
 * OPR is the comparison operator which could be on the following:
 * 	&gt;, &lt;, = , !=, IN, BETWEEN
 * </pre>
 */
public class NameValue extends AbstractNode implements FilterProvider {

    private Name name;

    private ComparisonOp op;

    private ConstValue value;

    private Range range;

    private ListConst valueList;
    private RangeQuery.Builder r;

    public NameValue(InputStream is) throws ParserException {
        super(is);
    }

    @Override
    protected void _parse() throws Exception {
        this.name = new Name(is);
        this.op = new ComparisonOp(is);

        if (this.op.getOperator().equals(Operators.BETWEEN.value())) {
            this.range = new Range(is);
        }
        if (this.op.getOperator().equals(Operators.IN.value())) {
            this.valueList = new ListConst(is);
        } else {
            this.value = new ConstValue(is);
        }
    }

    @Override
    public String toString() {
        return "" + name + op + value;
    }

    /**
     * @return the name
     */
    public Name getName() {
        return name;
    }

    /**
     * @return the op
     */
    public ComparisonOp getOp() {
        return op;
    }

    /**
     * @return the value
     */
    public ConstValue getValue() {
        return value;
    }

    @Override
    public Query getFilterBuilder() {
        if (op.getOperator().equals(Operators.EQUALS.value())) {
            return Query.of(
                    q ->
                            q.queryString(
                                    qs ->
                                            qs.query(
                                                    name.getName()
                                                            + ":"
                                                            + value.getValue().toString())));
        } else if (op.getOperator().equals(Operators.BETWEEN.value())) {
            return Query.of(
                    q ->
                            q.range(
                                    RangeQuery.of(
                                            r ->
                                                    r.term(
                                                            tr ->
                                                                    tr.field(name.getName())
                                                                            .gte(
                                                                                    this.range
                                                                                            .getLow())
                                                                            .lte(
                                                                                    this.range
                                                                                            .getHigh())))));
        } else if (op.getOperator().equals(Operators.IN.value())) {
            // Convert valueList.getList() to List<FieldValue> for ES8
            List<Object> rawList = valueList.getList();
            List<FieldValue> fieldValues = rawList.stream().map(FieldValue::of).toList();
            return Query.of(
                    q ->
                            q.terms(
                                    t ->
                                            t.field(name.getName() + ".keyword")
                                                    .terms(tt -> tt.value(fieldValues))));
        } else if (op.getOperator().equals(Operators.NOT_EQUALS.value())) {
            return Query.of(
                    q ->
                            q.bool(
                                    b ->
                                            b.mustNot(
                                                    mn ->
                                                            mn.queryString(
                                                                    qs ->
                                                                            qs.query(
                                                                                    name.getName()
                                                                                            + ":"
                                                                                            + value.getValue()
                                                                                                    .toString())))));
        } else if (op.getOperator().equals(Operators.GREATER_THAN.value())) {
            return Query.of(
                    q ->
                            q.range(
                                    RangeQuery.of(
                                            r ->
                                                    r.term(
                                                            tr ->
                                                                    tr.field(name.getName())
                                                                            .gt(
                                                                                    value.getValue()
                                                                                            .toString())))));
        } else if (op.getOperator().equals(Operators.LESS_THAN.value())) {
            return Query.of(
                    q ->
                            q.range(
                                    RangeQuery.of(
                                            r ->
                                                    r.term(
                                                            tr ->
                                                                    tr.field(name.getName())
                                                                            .lt(
                                                                                    value.getValue()
                                                                                            .toString())))));
        }
        return Query.of(q -> q.matchAll(m -> m));
    }
}
