package com.ginkgo.calcite.server;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;

class TableImpl extends AbstractTable
        implements ModifiableTable, ProjectableFilterableTable {
    TableImpl() {}

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
                .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("name", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .build();
    }

    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
                                     int[] projects) {
        throw new UnsupportedOperationException();
    }

    public Collection getModifiableCollection() {
        throw new UnsupportedOperationException();
    }

    public TableModify toModificationRel(RelOptCluster cluster,
                                         RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child,
                                         TableModify.Operation operation, List<String> updateColumnList,
                                         List<RexNode> sourceExpressionList, boolean flattened) {
        return LogicalTableModify.create(table, catalogReader, child, operation,
                updateColumnList, sourceExpressionList, flattened);
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                        SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    public Type getElementType() {
        return Object.class;
    }

    public Expression getExpression(SchemaPlus schema, String tableName,
                                    Class clazz) {
        throw new UnsupportedOperationException();
    }
}