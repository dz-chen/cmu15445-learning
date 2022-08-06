[toc]

# 实现细节注意
## 重要数据结构
### Value=> sql数据的抽象类
```
class Value{
  // 成员变量 
 protected:
  // The actual value item
  union Val {
    int8_t boolean_;
    int8_t tinyint_;
    int16_t smallint_;
    int32_t integer_;
    int64_t bigint_;
    double decimal_;
    uint64_t timestamp_;
    char *varlen_;
    const char *const_varlen_;
  } value_;   // 存储实际数据

  union {
    uint32_t len_;
    TypeId elem_type_id_;
  } size_;  // 字节数,或者typeid

  bool manage_data_;  // true则数据使用varlen_存储,size_为len_; false则数据使用基本类型存储,size_为typeid
  // The data type
  TypeId type_id_;  // 数据类型
}
```
### IntegerType、DecimalType等(注意与Value的关系)
以 IntegerType 为例：
```
class IntegerType : public IntegerParentType {
    ......
};
```
IntegerType表示数据库中的整型，它的继承关系如下：`IntegerType->IntegerParentType->NumericType->Type`;  
```
class Type{
  // 单例...
  inline static Type *GetInstance(TypeId type_id) { return k_types[type_id]; }
  virtual Value Add(const Value &left, const Value &right);
  virtual void SerializeTo(const Value &val, char *storage);

  // The actual type ID
  TypeId type_id_;
  // Singleton instances.
  static Type *k_types[14];  // k_types[]的初始化见 type.cpp
};
```
### Tuple => 对元组数据的包装
```
/**
 * Tuple format(这是指磁盘上的元组数据格式):
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
 */
class Tuple {
 private:
  // Get the starting storage address of specific column
  const char *GetDataPtr(const Schema *schema, uint32_t column_idx) const;

  bool allocated_{false};  // is allocated?
  RID rid_{};              // if pointing to the table heap, the rid is valid
  uint32_t size_{0};
  char *data_{nullptr};     // 指向真正元组数据的指针
};
```
### Column => 描述Schema的一列
```
class Column {
  /** Column name. */
  std::string column_name_;

  /** Column value's type. */
  TypeId column_type_;

  /** For a non-inlined column, this is the size of a pointer. Otherwise, the size of the fixed length column. */
  uint32_t fixed_length_;

  /** For an inlined column, 0. Otherwise, the length of the variable length column. */
  uint32_t variable_length_{0};

  /** Column offset in the tuple. */
  uint32_t column_offset_{0};

  /** Expression used to create this column => 如何理解? **/
  const AbstractExpression *expr_; 
};
```
### Schema => 描述一张表的所有(或部分)属性列
```
class Schema {
 private:
  /** Fixed-length column size, i.e. the number of bytes used by one tuple. */
  uint32_t length_;

  /** All the columns in the schema, inlined and uninlined. */
  std::vector<Column> columns_;

  /** True if all the columns are inlined, false otherwise. */
  bool tuple_is_inlined_;

  /** Indices of all uninlined columns. */
  std::vector<uint32_t> uninlined_columns_;
};
```
### ColumnValueExpression => 
注意其中 tuple_idx_ 成员的含义（仅在join时使用）
```
/**
 * ColumnValueExpression maintains the tuple index and column index relative to a particular schema or join.
 */
class ColumnValueExpression : public AbstractExpression {
 private:
  /** Tuple index 0 = left side of join, tuple index 1 = right side of join */
  uint32_t tuple_idx_;
  /** Column index refers to the index within the schema of the tuple, e.g. schema {A,B,C} has indexes {0,1,2} */
  uint32_t col_idx_;
};
```
### AbstractPlanNode
planNode代表当前查询的输出...  
```
class AbstractPlanNode {
 public:
  /** @return the child of this plan node at index child_idx */
  const AbstractPlanNode *GetChildAt(uint32_t child_idx) const { return children_[child_idx]; }

  /** @return the children of this plan node */
  const std::vector<const AbstractPlanNode *> &GetChildren() const { return children_; }

  /** @return the type of this plan node */
  virtual PlanType GetType() const = 0;

 private:
  /**
   * The schema for the output of this plan node. In the volcano model, every plan node will spit out tuples, and this tells you what schema this plan node's tuples will have.
   */
  const Schema *output_schema_;
  
  /** The children of this plan node. */
  std::vector<const AbstractPlanNode *> children_;
};
```


## SYSTEM CATALOG
- 
## executorEngine,executorContext,executorFactory,plan,executor的关系
```
executor => plan => expression
```
- 每个`XxxPlanNode`最重要的就是其中的`chidren`以及`predicate_`.
```
1.chidren 用于获取当前plan需要的输入数据(比如对于join,就需要两个child,一个返回左表的数据,一个返回右表的数据);
2.predicate_代表XxxPlanNode返回的数据需要满足的条件(即SQL的where子句),predicate_是一颗表达式树;
```
- 满足`XxxPlanNode->predicate_`的数据,才会通过`XxxExecutor->Next()`返回;
- `XxxExecutor->Next()应该是一个递归的调用`,当前层Next()的数据,依赖于`XxxPlanNode->children->predicate_`返回的数据;
- Each executor is responsible for processing a single plan node type. They accept tuples from their children and give tuples to their parent. They may rely on conventions about the ordering of their children.
- We also assume executors are single-threaded throughout the entire project.

## expression
- ColumnValueExpression->Evaluate(tuple,schema),求值即`返回tuple中某个属性的值`(该属性的col_index是ColumnValueExpression的成员变量!)
- 


# 知识点积累
## type.h/type.cpp中单例模式分析（TODO）

# TODO
## Catalog.CreateTable()时,如何解决TableHeap的初始化问题(fixed)?
见代码...

## Catalog.CreateIndex()时,Index是抽象类,如何创建IndexInfo的实例(fixed)?
见代码...

## TableHeap源码待学习(TODO)

## executor的children_,plan_的children_,expression的children_有何区别与联系(TODO)?

## index_scan_executor中IndexScanBPTreeIdxType修改(TODO)?
是否可借鉴:[nested_index_join_executor.cpp](https://github.com/yzhao001/15445-2020FALL/blob/master/src/execution/nested_index_join_executor.cpp)

## 在部分属性列上存在取值相同的可能,因此B+树应该支持一个key对应多个val,待修改(TODO)?

## b_plus_tree_index源码待学习(TODO)

## update_executor时,索引更新部分未调试成功(TODO,重要)?
目前看应该是index1在插入/删除的过程中size_变小了,B+树实现上可能有问题...

## /nested_index_join_executor中在使用plan_时是否需要考虑该plan_的子结点？
[](https://github.com/yzhao001/15445-2020FALL/blob/master/src/execution/nested_index_join_executor.cpp)这个是考虑了的...

## 本lab中内表外表的含义(fixed)
`本lab中的连接都是内连接`(只返回取值相等的结果);  
代码中`内表只需记住是使用索引查找数据`,`外表是使用loop查找的数据`;  


## volcano model

## executor与plan的关系
本质上是一一对应的,一个executor的child_executor就对应了executor->plan_的child_plan

## Column中的expr_如何理解(fixed,重要)
expr_就是创建当前Column的expression;  
何时传入:可参考 executor_test.cpp中的 `MakeOutputSchema()`函数;  
```
  const Schema *MakeOutputSchema(const std::vector<std::pair<std::string, const AbstractExpression *>> &exprs) {
    std::vector<Column> cols;
    cols.reserve(exprs.size());
    for (const auto &input : exprs) {
      if (input.second->GetReturnType() != TypeId::VARCHAR) {
        // 此处实际调用了 Column() 的构造函数, input.second 即 expr_
        cols.emplace_back(input.first, input.second->GetReturnType(), input.second);
      } else {
        cols.emplace_back(input.first, input.second->GetReturnType(), MAX_VARCHAR_SIZE, input.second);
      }
    }
    allocated_output_schemas_.emplace_back(std::make_unique<Schema>(cols));
    return allocated_output_schemas_.back().get();
  }
```
作用:`通过col.GetExpr()可直接或得需要的expression` => 见 nested_index_join_executor.cpp以及aggregation_executor.cpp;  



## Column不知道自己在第几列,ColumnValueExpression才知道在第几列(fixed,重要)
重点关注`ColumnValueExpression`的成员`tuple_idx_`;  
每个ColumnValueExpression对应了一个属性值,tuple_idx_描述了该属性值在第几列,从而Evaluate(tuple,schema)时能返回具体的值!!!  

## 当有嵌套executor时,ExecutorContext是如何保证被正确获取的?

## group by与having(fixed)
WHERE子句和Having子句功能是相同的,都是做数据筛选的;`普通条件`的判断建议放在WHERE子句中 <=> `聚合函数和其他的数据`的条件判断需要放在Having子句中;  
having子句或许可以看成是一个`分组后,再筛选一次的操作`(组内的操作);  
`Having子句要和GROUP BY子句联合起来才能使用`,Having子句不能单独使用;  
示例:查询平均底薪超过2000的部门:  
```
SELECT deptno
FROM t_emp
GROUP BY deptno HAVING AVG(sal)>=2000;
```
更多参考:[数据库的高级查询四:Having子句](https://blog.csdn.net/csucsgoat/article/details/115380747)

## bustub::HashUtil待学习

## 理解AggregateKey,MakeKey(); AggregateValue,MakeVal();以及SimpleAggregationHashTable(fixed,重要)
**AggregateKey与MakeKey()**  
AggregateKey用于确定group by分类后的分组,它是SimpleAggregationHashTable的key;  
`MakeKey(tuple)返回当前分组中group by 子句中的列的取值,这些值组合起来作为 SimpleAggregationHashTable 的key`,从而进行分组;  
对于不含group by子句的聚合,每次调用 MakeKey(tuple)返回的都是同一个 key,也就相当于只有一个分组;  

**AggregateValue与MakeValue()**  
AggregateValue有两处被使用:  
1.表示当前元组中参与聚合的列的取值 => 这种情况下是 MakeVal()函数的返回值;  
2.表示当前group by分组内 count,sum,min,nax统计结果,这种情况下是 SimpleAggregationHashTable 的 val;  

详见:`aggregation_plan.h`、`aggregation_executor.h`;  

## aggregation的实现逻辑,如何统计聚合结果的(fixed,重要)
逻辑如下,核心就是 CombineAggregateValues()!!!  
```
aht_.InsertCombine(agg_key,agg_val)
  CombineAggregateValues(&ht[agg_key], agg_val);
```

## 实现limit_executor(这个相对比较简单,但是没有测试代码,需自行完成...)

# tmp
## 复制粘贴常用
```
// cmake 生成makefile(debug版)
cmake -DCMAKE_BUILD_TYPE=DEBUG ..

// catalog_test 执行及调试
make -j4 catalog_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/catalog_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/catalog_test --gtest_filter=CatalogTest.CreateTableTest
b catalog_test.cpp:68


// executor_test 执行及调试
make -j4 executor_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4  ./test/executor_test
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/executor_test --gtest_filter=ExecutorTest.SimpleUpdateTest
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 gdb --args ./test/executor_test --gtest_filter=ExecutorTest.SimpleAggregationTest
b executor_test.cpp:808
```