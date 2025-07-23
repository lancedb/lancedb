# LanceDB Java SDK

这是LanceDB的Java SDK，提供了与LanceDB数据库交互的Java API。

## 功能特性

- 连接管理
- 表操作（创建、打开、删除）
- 数据操作（添加、更新、删除、查询）
- 向量搜索
- **Merge Insert操作** - 根据键列合并数据

## Merge Insert 功能

Merge Insert是LanceDB的一个重要功能，允许你根据指定的键列来合并新数据到现有表中。

### 基本用法

```java
import com.lancedb.lancedb.*;

// 连接到数据库
Connection connection = Connection.connect("path/to/database");

// 打开表
Table table = connection.openTable("my_table");

// 创建merge insert构建器，使用"id"列作为匹配键
MergeBuilder mergeBuilder = table.mergeInsert("id");

// 配置merge行为
mergeBuilder
    .whenMatchedUpdateAll()  // 匹配的行将被更新
    .whenNotMatchedInsertAll();  // 不匹配的行将被插入

// 准备新数据
List<Map<String, Object>> newData = Arrays.asList(
    Map.of("id", 1, "name", "Alice", "age", 25),
    Map.of("id", 2, "name", "Bob", "age", 30),
    Map.of("id", 3, "name", "Charlie", "age", 35)
);

// 执行merge操作
MergeResult result = mergeBuilder.execute(newData);

// 查看结果
System.out.println("插入的行数: " + result.getNumInsertedRows());
System.out.println("更新的行数: " + result.getNumUpdatedRows());
System.out.println("删除的行数: " + result.getNumDeletedRows());
System.out.println("操作版本: " + result.getVersion());
```

### 高级用法

#### 条件更新

```java
// 只有当目标行的last_update小于源行的last_update时才更新
mergeBuilder.whenMatchedUpdateAll(
    Optional.of("target.last_update < source.last_update")
);
```

#### 条件删除

```java
// 删除目标表中不在源数据中的行，但只删除age大于50的行
mergeBuilder.whenNotMatchedBySourceDelete(
    Optional.of("target.age > 50")
);
```

#### 设置超时

```java
// 设置操作超时为60秒
mergeBuilder.timeout(Duration.ofSeconds(60));
```

#### 完整示例

```java
import com.lancedb.lancedb.*;
import java.time.Duration;
import java.util.*;

public class MergeExample {
    public static void main(String[] args) {
        // 连接数据库
        Connection connection = Connection.connect("./data");
        
        // 创建表（如果不存在）
        List<Map<String, Object>> initialData = Arrays.asList(
            Map.of("id", 1, "name", "Alice", "age", 25, "last_update", "2024-01-01"),
            Map.of("id", 2, "name", "Bob", "age", 30, "last_update", "2024-01-01")
        );
        
        Table table = connection.createTable("users", initialData, CreateTableMode.CREATE);
        
        // 准备更新的数据
        List<Map<String, Object>> updateData = Arrays.asList(
            Map.of("id", 1, "name", "Alice Smith", "age", 26, "last_update", "2024-01-15"),  // 更新现有记录
            Map.of("id", 3, "name", "Charlie", "age", 35, "last_update", "2024-01-15"),      // 插入新记录
            Map.of("id", 4, "name", "David", "age", 40, "last_update", "2024-01-15")         // 插入新记录
        );
        
        // 执行merge操作
        MergeResult result = table.mergeInsert("id")
            .whenMatchedUpdateAll(Optional.of("target.last_update < source.last_update"))
            .whenNotMatchedInsertAll()
            .timeout(Duration.ofSeconds(30))
            .execute(updateData);
        
        // 输出结果
        System.out.println("Merge操作完成:");
        System.out.println("- 插入的行数: " + result.getNumInsertedRows());
        System.out.println("- 更新的行数: " + result.getNumUpdatedRows());
        System.out.println("- 删除的行数: " + result.getNumDeletedRows());
        System.out.println("- 操作版本: " + result.getVersion());
        System.out.println("- 总影响行数: " + result.getTotalAffectedRows());
    }
}
```

## API 参考

### MergeBuilder

#### 构造函数
- `MergeBuilder(Table table, String[] onColumns)` - 创建merge构建器

#### 方法
- `whenMatchedUpdateAll()` - 设置匹配行更新行为（无条件）
- `whenMatchedUpdateAll(Optional<String> condition)` - 设置匹配行更新行为（有条件）
- `whenNotMatchedInsertAll()` - 设置不匹配行插入行为
- `whenNotMatchedBySourceDelete()` - 设置源数据中不存在的目标行删除行为（无条件）
- `whenNotMatchedBySourceDelete(Optional<String> filter)` - 设置源数据中不存在的目标行删除行为（有条件）
- `timeout(Duration timeout)` - 设置操作超时
- `execute(List<Map<String, Object>> newData)` - 执行merge操作

### MergeResult

#### 属性
- `getVersion()` - 获取操作的提交版本
- `getNumInsertedRows()` - 获取插入的行数
- `getNumUpdatedRows()` - 获取更新的行数
- `getNumDeletedRows()` - 获取删除的行数
- `getTotalAffectedRows()` - 获取总影响行数

## 注意事项

1. **性能考虑**: Merge操作是批量操作，对于大量数据比逐个更新更高效
2. **事务性**: Merge操作是原子的，要么全部成功，要么全部失败
3. **版本控制**: 每次merge操作都会创建一个新的表版本
4. **超时设置**: 默认有30秒超时，可以根据数据量调整
5. **条件语法**: 条件使用SQL语法，使用`target.`前缀引用目标表，`source.`前缀引用源数据

## 错误处理

```java
try {
    MergeResult result = table.mergeInsert("id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute(newData);
} catch (LanceDBException e) {
    System.err.println("Merge操作失败: " + e.getMessage());
}
``` 