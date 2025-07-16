#!/bin/bash

# LanceDB Java 模块编译脚本
# 这个脚本用于编译 Java 模块和相关的 Rust JNI 代码

set -e  # 遇到错误时退出

echo "🚀 开始编译 LanceDB Java 模块..."

# 检查必要的工具
check_requirements() {
    echo "📋 检查编译要求..."
    
    # 检查 Java
    if ! command -v java &> /dev/null; then
        echo "❌ 错误: 未找到 Java，请安装 Java 11 或更高版本"
        exit 1
    fi
    
    # 检查 Maven
    if ! command -v mvn &> /dev/null; then
        echo "❌ 错误: 未找到 Maven，请安装 Maven"
        exit 1
    fi
    
    # 检查 Rust
    if ! command -v cargo &> /dev/null; then
        echo "❌ 错误: 未找到 Rust，请安装 Rust"
        exit 1
    fi
    
    echo "✅ 所有要求都已满足"
}

# 编译 Rust JNI 模块
build_rust_jni() {
    echo "🔨 编译 Rust JNI 模块..."
    cd lancedb-jni
    
    # 设置环境变量
    export CARGO_HOME="$HOME/.cargo"
    export RUST_BACKTRACE=1
    
    # 编译
    cargo build --release
    
    if [ $? -eq 0 ]; then
        echo "✅ Rust JNI 模块编译成功"
    else
        echo "❌ Rust JNI 模块编译失败"
        exit 1
    fi
    
    cd ..
}

# 编译 Java 模块
build_java_module() {
    echo "☕ 编译 Java 模块..."
    
    # 清理之前的构建
    mvn clean
    
    # 编译
    mvn compile
    
    if [ $? -eq 0 ]; then
        echo "✅ Java 模块编译成功"
    else
        echo "❌ Java 模块编译失败"
        exit 1
    fi
}

# 运行测试
run_tests() {
    echo "🧪 运行测试..."
    
    mvn test
    
    if [ $? -eq 0 ]; then
        echo "✅ 所有测试通过"
    else
        echo "❌ 测试失败"
        exit 1
    fi
}

# 打包
package_module() {
    echo "📦 打包模块..."
    
    mvn package -DskipTests
    
    if [ $? -eq 0 ]; then
        echo "✅ 模块打包成功"
        echo "📁 JAR 文件位置: target/lancedb-java-*.jar"
    else
        echo "❌ 模块打包失败"
        exit 1
    fi
}

# 运行示例
run_example() {
    echo "🎯 运行示例程序..."
    
    # 检查示例文件是否存在
    if [ ! -f "examples/MergeExample.java" ]; then
        echo "⚠️  示例文件不存在，跳过示例运行"
        return
    fi
    
    # 编译示例
    javac -cp "target/classes:target/dependency/*" examples/MergeExample.java
    
    if [ $? -eq 0 ]; then
        echo "✅ 示例编译成功"
        
        # 运行示例
        echo "🚀 运行 MergeExample..."
        java -cp "target/classes:target/dependency/*:examples" MergeExample
        
        if [ $? -eq 0 ]; then
            echo "✅ 示例运行成功"
        else
            echo "❌ 示例运行失败"
        fi
    else
        echo "❌ 示例编译失败"
    fi
}

# 显示帮助信息
show_help() {
    echo "LanceDB Java 模块编译脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  --help, -h     显示此帮助信息"
    echo "  --check        只检查编译要求"
    echo "  --rust-only    只编译 Rust JNI 模块"
    echo "  --java-only    只编译 Java 模块"
    echo "  --test-only    只运行测试"
    echo "  --example      编译并运行示例"
    echo "  --all          完整编译流程（默认）"
    echo ""
    echo "示例:"
    echo "  $0              # 完整编译流程"
    echo "  $0 --check      # 检查要求"
    echo "  $0 --test-only  # 只运行测试"
}

# 主函数
main() {
    case "${1:---all}" in
        --help|-h)
            show_help
            exit 0
            ;;
        --check)
            check_requirements
            ;;
        --rust-only)
            check_requirements
            build_rust_jni
            ;;
        --java-only)
            check_requirements
            build_java_module
            ;;
        --test-only)
            check_requirements
            run_tests
            ;;
        --example)
            check_requirements
            build_rust_jni
            build_java_module
            run_example
            ;;
        --all)
            check_requirements
            build_rust_jni
            build_java_module
            run_tests
            package_module
            echo ""
            echo "🎉 编译完成！"
            echo ""
            echo "📚 使用说明:"
            echo "  1. 查看 README.md 了解详细使用方法"
            echo "  2. 运行示例: $0 --example"
            echo "  3. 运行测试: $0 --test-only"
            ;;
        *)
            echo "❌ 未知选项: $1"
            show_help
            exit 1
            ;;
    esac
}

# 运行主函数
main "$@" 