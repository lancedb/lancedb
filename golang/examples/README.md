# LanceDB Go SDK Examples

This directory contains comprehensive examples demonstrating the various capabilities of the LanceDB Go SDK. Each example focuses on different aspects of using LanceDB in production applications.

## 📚 Available Examples

### 1. Basic CRUD Operations (`basic_crud.go`)
**Demonstrates:** Fundamental database operations - Create, Read, Update, Delete
- Database connection and table creation
- Schema definition with multiple data types
- Inserting, querying, updating, and deleting records
- Basic error handling and resource management

```bash
go run basic_crud.go
```

### 2. Vector Search (`vector_search.go`)
**Demonstrates:** Comprehensive vector similarity search capabilities
- Creating and storing vector embeddings
- Basic and advanced vector similarity search
- Different K values and search configurations
- Vector search with metadata filtering
- Performance benchmarking and optimization

```bash
go run vector_search.go
```

### 3. Hybrid Search (`hybrid_search.go`)
**Demonstrates:** Combining vector search with traditional filtering
- E-commerce product catalog with vectors and metadata
- Vector search combined with SQL-like filters
- Multi-modal query patterns
- Recommendation system implementations
- Cross-category and similarity-based recommendations

```bash
go run hybrid_search.go
```

### 4. Index Management (`index_management.go`)
**Demonstrates:** Creating and managing different types of indexes
- Vector indexes: IVF-PQ, IVF-Flat, HNSW-PQ for different use cases
- Scalar indexes: BTree for range queries, Bitmap for categorical data
- Full-text search indexes for text content
- Index performance comparison and optimization
- Best practices for index selection

```bash
go run index_management.go
```

### 5. Batch Operations (`batch_operations.go`)
**Demonstrates:** Efficient bulk data operations
- Different batch insertion strategies and performance comparison
- Batch update and delete operations
- Memory-efficient processing of large datasets
- Concurrent batch operations with goroutines
- Error handling and recovery patterns for batch operations

```bash
go run batch_operations.go
```

### 6. Storage Configuration (`storage_configuration.go`)
**Demonstrates:** Local and cloud storage configurations
- Local file system storage with optimization settings
- AWS S3 configuration with authentication methods
- MinIO object storage for local development
- Storage performance comparison and optimization
- Error handling and fallback strategies

```bash
go run storage_configuration.go
```

### 7. Cloud Deployment (`cloud_deployment.go`)
**Demonstrates:** Production-ready cloud deployment patterns
- Production AWS S3 configurations with security best practices
- Multi-environment deployment strategies (dev/staging/prod)
- High-availability and disaster recovery patterns
- Security and compliance configurations
- Cost optimization strategies
- Monitoring and observability setup
- Deployment automation patterns

```bash
go run cloud_deployment.go
```

## 🚀 Getting Started

### Prerequisites

1. **Go 1.19+** installed on your system
2. **LanceDB Go SDK dependencies** (install with `go mod tidy`)
3. **Optional**: AWS credentials for S3 examples
4. **Optional**: MinIO server for object storage examples

### Installation

1. Clone the LanceDB repository:
```bash
git clone https://github.com/lancedb/lancedb.git
cd lancedb/golang
```

2. Install dependencies:
```bash
go mod tidy
```

3. Build the Go SDK (if needed):
```bash
make build
```

### Running Examples

Each example is self-contained and can be run independently:

```bash
# Basic CRUD operations
go run examples/basic_crud.go

# Vector search capabilities
go run examples/vector_search.go

# Hybrid search patterns
go run examples/hybrid_search.go

# Index management
go run examples/index_management.go

# Batch operations
go run examples/batch_operations.go

# Storage configuration
go run examples/storage_configuration.go

# Cloud deployment patterns
go run examples/cloud_deployment.go
```

## 🔧 Configuration

### Local Examples
Most examples work out-of-the-box with local storage and don't require additional configuration.

### AWS S3 Examples
For S3-related examples, you'll need AWS credentials configured:

1. **IAM Role (Recommended for EC2/ECS)**:
   - No additional configuration needed
   - Examples will use the instance's IAM role

2. **AWS Credentials File**:
   ```bash
   aws configure
   # Or set up ~/.aws/credentials
   ```

3. **Environment Variables**:
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_REGION=us-east-1
   ```

### MinIO Examples
For MinIO examples, start a local MinIO server:

```bash
# Using Docker
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  quay.io/minio/minio server /data --console-address ":9001"

# Create test bucket via MinIO console at http://localhost:9001
```

## 📖 Example Details

### Basic CRUD (`basic_crud.go`)
```
📊 Features:
• Database connection management
• Table schema creation with multiple data types
• Record insertion, querying, updating, deletion
• Error handling and resource cleanup
• Performance timing and metrics

🎯 Use Cases:
• Learning basic LanceDB operations
• Understanding schema design
• Building simple applications
```

### Vector Search (`vector_search.go`)
```
📊 Features:
• Vector embedding generation and storage
• Similarity search with configurable parameters
• Performance benchmarking across different K values
• Metadata filtering combined with vector search
• Query optimization techniques

🎯 Use Cases:
• Semantic search applications
• Recommendation systems
• Content similarity matching
• AI/ML model integration
```

### Hybrid Search (`hybrid_search.go`)
```
📊 Features:
• Product catalog with vectors and structured data
• Combined vector + metadata filtering
• E-commerce search scenarios
• Recommendation patterns
• Multi-modal query optimization

🎯 Use Cases:
• E-commerce product search
• Content discovery platforms
• Recommendation engines
• Advanced search applications
```

### Index Management (`index_management.go`)
```
📊 Features:
• Vector indexes: IVF-PQ, IVF-Flat, HNSW-PQ
• Scalar indexes: BTree, Bitmap, Label List
• Full-text search indexes
• Performance comparison and analysis
• Index selection best practices

🎯 Use Cases:
• Query performance optimization
• Large-scale vector search
• Mixed workload optimization
• Production database tuning
```

### Batch Operations (`batch_operations.go`)
```
📊 Features:
• Bulk insertion strategies and performance
• Memory-efficient large dataset processing
• Concurrent processing with goroutines
• Error handling and recovery patterns
• Resource management and optimization

🎯 Use Cases:
• Data migration and ETL
• Large-scale data ingestion
• Bulk data processing
• Performance-critical applications
```

### Storage Configuration (`storage_configuration.go`)
```
📊 Features:
• Local storage optimization
• AWS S3 configuration patterns
• MinIO setup for development
• Storage performance comparison
• Error handling and fallback strategies

🎯 Use Cases:
• Development environment setup
• Cloud storage integration
• Storage optimization
• Multi-environment deployment
```

### Cloud Deployment (`cloud_deployment.go`)
```
📊 Features:
• Production AWS configurations
• Multi-environment strategies
• Security and compliance setup
• Cost optimization techniques
• Monitoring and observability
• Deployment automation patterns

🎯 Use Cases:
• Production deployments
• Enterprise applications
• Compliance requirements
• Cost management
• Operational excellence
```

## 🛠 Development Tips

### Running with Custom Parameters
Many examples accept environment variables for customization:

```bash
# Custom database path
DATABASE_PATH="/tmp/my-lancedb" go run examples/basic_crud.go

# Custom AWS region
AWS_REGION="us-west-2" go run examples/storage_configuration.go

# Enable debug logging
DEBUG=true go run examples/vector_search.go
```

### Performance Testing
Use the `time` command to measure example execution:

```bash
time go run examples/batch_operations.go
```

### Memory Profiling
Enable Go's built-in profiling for memory analysis:

```bash
go run -pprof examples/batch_operations.go
```

## 📚 Learning Path

**Recommended order for learning:**

1. **Start with `basic_crud.go`** - Learn fundamental operations
2. **Try `vector_search.go`** - Understand vector operations  
3. **Explore `hybrid_search.go`** - See real-world patterns
4. **Study `index_management.go`** - Optimize performance
5. **Practice `batch_operations.go`** - Handle large datasets
6. **Configure `storage_configuration.go`** - Set up storage
7. **Deploy with `cloud_deployment.go`** - Go to production

## 🔍 Troubleshooting

### Common Issues

**Connection Errors:**
```
Error: failed to connect to database
Solution: Check file permissions and disk space for local storage
```

**AWS S3 Errors:**
```
Error: failed to connect to S3
Solution: Verify AWS credentials and bucket permissions
```

**MinIO Errors:**
```
Error: connection refused
Solution: Ensure MinIO server is running on localhost:9000
```

**Build Errors:**
```
Error: undefined: lancedb
Solution: Run 'go mod tidy' and 'make build'
```

### Getting Help

1. **Check the main README**: `../README.md`
2. **Review error messages**: Examples include detailed error handling
3. **Enable debug logging**: Set `DEBUG=true` environment variable
4. **Check dependencies**: Run `go mod verify`

## 🤝 Contributing

To add new examples:

1. Create a new `.go` file in this directory
2. Follow the existing code structure and documentation style
3. Include comprehensive comments and error handling
4. Add the example to this README with description
5. Test thoroughly with different scenarios

## 📄 License

All examples are provided under the same license as the LanceDB project.
