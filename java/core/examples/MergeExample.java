import com.lancedb.lancedb.*;
import java.time.Duration;
import java.util.*;

/**
 * MergeBuilder使用示例
 * 
 * 这个示例演示了如何使用LanceDB的Merge Insert功能来合并数据。
 * Merge Insert允许你根据指定的键列来合并新数据到现有表中。
 */
public class MergeExample {
    
    public static void main(String[] args) {
        try {
            // 连接到数据库（使用当前目录下的data文件夹）
            System.out.println("连接到数据库...");
            Connection connection = Connection.connect("./data");
            
            // 创建初始数据
            List<Map<String, Object>> initialData = Arrays.asList(
                Map.of("id", 1, "name", "Alice", "age", 25, "department", "Engineering", "last_update", "2024-01-01"),
                Map.of("id", 2, "name", "Bob", "age", 30, "department", "Sales", "last_update", "2024-01-01"),
                Map.of("id", 3, "name", "Charlie", "age", 35, "department", "Marketing", "last_update", "2024-01-01")
            );
            
            // 创建表（如果不存在）
            System.out.println("创建用户表...");
            Table table = connection.createTable("users", initialData, CreateTableMode.CREATE);
            
            System.out.println("初始表创建完成，包含 " + table.countRows() + " 行数据");
            
            // 示例1：基本merge操作 - 更新现有记录和插入新记录
            System.out.println("\n=== 示例1：基本merge操作 ===");
            List<Map<String, Object>> updateData1 = Arrays.asList(
                Map.of("id", 1, "name", "Alice Smith", "age", 26, "department", "Engineering", "last_update", "2024-01-15"),  // 更新现有记录
                Map.of("id", 4, "name", "David", "age", 40, "department", "HR", "last_update", "2024-01-15")                    // 插入新记录
            );
            
            MergeResult result1 = table.mergeInsert("id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute(updateData1);
            
            System.out.println("Merge结果:");
            System.out.println("- 插入的行数: " + result1.getNumInsertedRows());
            System.out.println("- 更新的行数: " + result1.getNumUpdatedRows());
            System.out.println("- 删除的行数: " + result1.getNumDeletedRows());
            System.out.println("- 操作版本: " + result1.getVersion());
            
            // 示例2：条件更新 - 只有当目标行的last_update小于源行的last_update时才更新
            System.out.println("\n=== 示例2：条件更新 ===");
            List<Map<String, Object>> updateData2 = Arrays.asList(
                Map.of("id", 2, "name", "Bob Old", "age", 30, "department", "Sales", "last_update", "2023-12-01")  // 更旧的更新时间
            );
            
            MergeResult result2 = table.mergeInsert("id")
                .whenMatchedUpdateAll(Optional.of("target.last_update < source.last_update"))
                .whenNotMatchedInsertAll()
                .execute(updateData2);
            
            System.out.println("条件更新结果:");
            System.out.println("- 更新的行数: " + result2.getNumUpdatedRows() + " (由于条件不满足，应该为0)");
            
            // 示例3：条件删除 - 删除不在源数据中的目标行，但只删除特定条件的行
            System.out.println("\n=== 示例3：条件删除 ===");
            List<Map<String, Object>> updateData3 = Arrays.asList(
                Map.of("id", 1, "name", "Alice Smith", "age", 26, "department", "Engineering", "last_update", "2024-01-20")
            );
            
            MergeResult result3 = table.mergeInsert("id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .whenNotMatchedBySourceDelete(Optional.of("target.age > 30"))  // 只删除age大于30的行
                .execute(updateData3);
            
            System.out.println("条件删除结果:");
            System.out.println("- 更新的行数: " + result3.getNumUpdatedRows());
            System.out.println("- 删除的行数: " + result3.getNumDeletedRows() + " (应该删除id=2和id=3，因为age都大于30)");
            
            // 示例4：设置超时
            System.out.println("\n=== 示例4：设置超时 ===");
            List<Map<String, Object>> updateData4 = Arrays.asList(
                Map.of("id", 5, "name", "Eve", "age", 28, "department", "Finance", "last_update", "2024-01-25")
            );
            
            MergeResult result4 = table.mergeInsert("id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .timeout(Duration.ofSeconds(30))  // 30秒超时
                .execute(updateData4);
            
            System.out.println("超时设置结果:");
            System.out.println("- 插入的行数: " + result4.getNumInsertedRows());
            
            // 示例5：使用多个列作为匹配键
            System.out.println("\n=== 示例5：多列匹配 ===");
            List<Map<String, Object>> updateData5 = Arrays.asList(
                Map.of("id", 1, "name", "Alice Smith", "age", 27, "department", "Engineering", "last_update", "2024-01-30")
            );
            
            MergeResult result5 = table.mergeInsert("id", "name")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute(updateData5);
            
            System.out.println("多列匹配结果:");
            System.out.println("- 更新的行数: " + result5.getNumUpdatedRows());
            
            // 最终统计
            System.out.println("\n=== 最终统计 ===");
            System.out.println("表中的总行数: " + table.countRows());
            System.out.println("表的当前版本: " + table.getVersion());
            
            // 清理资源
            table.close();
            System.out.println("\n示例完成！");
            
        } catch (Exception e) {
            System.err.println("发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 