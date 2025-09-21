// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// Storage Configuration Example
//
// This example demonstrates comprehensive storage configuration capabilities
// with LanceDB using the Go SDK. It covers:
// - Local file system storage optimization
// - AWS S3 storage with various authentication methods
// - MinIO object storage for local development
// - Storage performance comparison and optimization
// - Error handling and fallback strategies
// - Best practices for different deployment scenarios

package main

import (
	"context"
	"fmt"
	lancedb "github.com/lancedb/lancedb/pkg"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

const (
	VectorDim     = 128
	SampleRecords = 1000
)

type StorageTestRecord struct {
	ID     int32
	Name   string
	Vector []float32
}

func main() {
	fmt.Println("💾 LanceDB Go SDK - Storage Configuration Example")
	fmt.Println("==================================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Local storage configurations
	fmt.Println("📋 Step 1: Local storage configurations...")
	if err := demonstrateLocalStorage(ctx); err != nil {
		log.Printf("Local storage demo failed: %v", err)
	}

	// AWS S3 storage configurations (simulated - requires actual AWS credentials)
	fmt.Println("\n📋 Step 2: AWS S3 storage configurations...")
	if err := demonstrateS3Storage(ctx); err != nil {
		log.Printf("S3 storage demo note: %v", err)
	}

	// MinIO storage configuration
	fmt.Println("\n📋 Step 3: MinIO storage configuration...")
	if err := demonstrateMinIOStorage(ctx); err != nil {
		log.Printf("MinIO storage demo note: %v", err)
	}

	// Storage performance comparison
	fmt.Println("\n📋 Step 4: Storage performance comparison...")
	if err := performStorageComparison(ctx); err != nil {
		log.Printf("Storage comparison failed: %v", err)
	}

	// Advanced storage configurations
	fmt.Println("\n📋 Step 5: Advanced storage configurations...")
	if err := demonstrateAdvancedConfigurations(ctx); err != nil {
		log.Printf("Advanced configurations failed: %v", err)
	}

	// Error handling and fallback strategies
	fmt.Println("\n📋 Step 6: Error handling and fallback strategies...")
	if err := demonstrateErrorHandling(ctx); err != nil {
		log.Printf("Error handling demo failed: %v", err)
	}

	fmt.Println("\n🎉 Storage configuration examples completed!")
	fmt.Println("==================================================")
}

func demonstrateLocalStorage(ctx context.Context) error {
	fmt.Println("  🗂️ Local File System Storage Configurations")

	// Configuration 1: Basic local storage
	fmt.Println("  🔹 Basic local storage")

	tempDir1, err := os.MkdirTemp("", "lancedb_local_basic_")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir1)

	conn1, err := lancedb.Connect(ctx, tempDir1, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to basic local storage: %w", err)
	}
	defer conn1.Close()

	fmt.Printf("    ✅ Basic local storage at: %s\n", tempDir1)

	// Configuration 2: Optimized local storage
	fmt.Println("\n  🔹 Optimized local storage with configuration")

	tempDir2, err := os.MkdirTemp("", "lancedb_local_optimized_")
	if err != nil {
		return fmt.Errorf("failed to create optimized temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir2)

	// Configure local storage options
	createDirs := true
	useMemoryMap := true
	syncWrites := false
	maxRetries := 3
	connectTimeout := 10

	localOptions := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			LocalConfig: &lancedb.LocalConfig{
				CreateDirIfNotExists: &createDirs,
				UseMemoryMap:         &useMemoryMap,
				SyncWrites:           &syncWrites,
			},
			BlockSize:      &[]int{1024 * 1024}[0], // 1MB blocks
			MaxRetries:     &maxRetries,
			ConnectTimeout: &connectTimeout, // 10 seconds
		},
	}

	conn2, err := lancedb.Connect(ctx, tempDir2, localOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to optimized local storage: %w", err)
	}
	defer conn2.Close()

	fmt.Printf("    ✅ Optimized local storage at: %s\n", tempDir2)

	// Test both configurations with sample data
	if err := testStorageConfiguration("Basic Local", conn1, ctx); err != nil {
		fmt.Printf("    ⚠️ Basic local storage test failed: %v\n", err)
	}

	if err := testStorageConfiguration("Optimized Local", conn2, ctx); err != nil {
		fmt.Printf("    ⚠️ Optimized local storage test failed: %v\n", err)
	}

	// Configuration 3: Custom directory structure
	fmt.Println("\n  🔹 Custom directory structure")

	customBase, err := os.MkdirTemp("", "lancedb_custom_")
	if err != nil {
		return fmt.Errorf("failed to create custom base directory: %w", err)
	}
	defer os.RemoveAll(customBase)

	// Create organized directory structure
	dbDir := filepath.Join(customBase, "databases", "production", "vector_db")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("failed to create custom directory structure: %w", err)
	}

	conn3, err := lancedb.Connect(ctx, dbDir, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to custom directory: %w", err)
	}
	defer conn3.Close()

	fmt.Printf("    ✅ Custom directory structure: %s\n", dbDir)

	fmt.Println("\n  💡 Local Storage Best Practices:")
	fmt.Println("    • Use SSD storage for better performance")
	fmt.Println("    • Enable memory mapping for read-heavy workloads")
	fmt.Println("    • Organize databases in logical directory structures")
	fmt.Println("    • Consider RAID configurations for redundancy")
	fmt.Println("    • Monitor disk space and implement cleanup policies")

	return nil
}

func demonstrateS3Storage(ctx context.Context) error {
	fmt.Println("  ☁️ AWS S3 Storage Configurations")

	// Note: These examples show configuration patterns but won't actually connect
	// without real AWS credentials and S3 buckets

	// Configuration 1: Basic S3 with access keys
	fmt.Println("  🔹 Basic S3 configuration with access keys")

	// Simulated credentials (would come from environment or configuration)
	accessKey := "AKIA..."   // Would be actual access key
	secretKey := "secret..." // Would be actual secret key
	region := "us-east-1"

	s3Options1 := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			S3Config: &lancedb.S3Config{
				AccessKeyId:     &accessKey,
				SecretAccessKey: &secretKey,
				Region:          &region,
			},
		},
	}

	fmt.Println("    📝 Configuration created (connection would require valid credentials)")
	printS3Config("Basic S3", s3Options1.StorageOptions.S3Config)

	// Configuration 2: S3 with session token (STS)
	fmt.Println("\n  🔹 S3 configuration with temporary credentials (STS)")

	sessionToken := "session..." // Would be actual session token
	s3Options2 := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			S3Config: &lancedb.S3Config{
				AccessKeyId:     &accessKey,
				SecretAccessKey: &secretKey,
				SessionToken:    &sessionToken,
				Region:          &region,
			},
		},
	}

	printS3Config("S3 with STS", s3Options2.StorageOptions.S3Config)

	// Configuration 3: S3 with AWS profile
	fmt.Println("\n  🔹 S3 configuration with AWS profile")

	profile := "production"
	s3Options3 := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			S3Config: &lancedb.S3Config{
				Profile: &profile,
				Region:  &region,
			},
		},
	}

	printS3Config("S3 with Profile", s3Options3.StorageOptions.S3Config)

	// Configuration 4: S3 with advanced options
	fmt.Println("\n  🔹 S3 configuration with advanced options")

	useSSL := true
	serverSideEncrypt := "AES256"
	storageClass := "STANDARD_IA"
	maxRetries := 5
	timeout := 30
	connectTimeout := 10

	s3Options4 := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			S3Config: &lancedb.S3Config{
				AccessKeyId:       &accessKey,
				SecretAccessKey:   &secretKey,
				Region:            &region,
				UseSSL:            &useSSL,
				ServerSideEncrypt: &serverSideEncrypt,
				StorageClass:      &storageClass,
			},
			MaxRetries:     &maxRetries,
			Timeout:        &timeout,
			ConnectTimeout: &connectTimeout,
		},
	}

	printS3Config("S3 Advanced", s3Options4.StorageOptions.S3Config)

	// Configuration 5: Anonymous S3 access for public buckets
	fmt.Println("\n  🔹 Anonymous S3 access for public buckets")

	anonymous := true
	s3Options5 := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			S3Config: &lancedb.S3Config{
				AnonymousAccess: &anonymous,
				Region:          &region,
			},
		},
	}

	printS3Config("Anonymous S3", s3Options5.StorageOptions.S3Config)

	fmt.Println("\n  💡 S3 Storage Best Practices:")
	fmt.Println("    • Use IAM roles in EC2/ECS environments when possible")
	fmt.Println("    • Enable server-side encryption for sensitive data")
	fmt.Println("    • Choose appropriate storage class based on access patterns")
	fmt.Println("    • Use same region as your application for lowest latency")
	fmt.Println("    • Implement proper retry logic with exponential backoff")
	fmt.Println("    • Monitor S3 costs and optimize with lifecycle policies")

	// Attempt connection (will fail without real credentials)
	fmt.Println("\n  🧪 Testing S3 connection (expected to fail without real credentials)...")
	_, err := lancedb.Connect(ctx, "s3://my-test-bucket/lancedb", s3Options1)
	if err != nil {
		fmt.Printf("    ⚠️ S3 connection failed as expected: %v\n", err)
		fmt.Println("    💡 This is normal - real AWS credentials would be needed for actual connection")
	}

	return nil
}

func demonstrateMinIOStorage(ctx context.Context) error {
	fmt.Println("  🪣 MinIO Object Storage Configuration")

	// MinIO configuration for local development
	fmt.Println("  🔹 MinIO configuration for local development")

	endpoint := "http://localhost:9000"
	accessKey := "minioadmin"
	secretKey := "minioadmin"
	forcePathStyle := true
	useSSL := false
	maxRetries := 5
	timeout := 30

	minioOptions := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			S3Config: &lancedb.S3Config{
				Endpoint:        &endpoint,
				AccessKeyId:     &accessKey,
				SecretAccessKey: &secretKey,
				ForcePathStyle:  &forcePathStyle, // Required for MinIO
				UseSSL:          &useSSL,
			},
			MaxRetries: &maxRetries,
			Timeout:    &timeout,
		},
	}

	fmt.Printf("    📝 MinIO configuration created:\n")
	fmt.Printf("      Endpoint: %s\n", endpoint)
	fmt.Printf("      Access Key: %s\n", accessKey)
	fmt.Printf("      Force Path Style: %t\n", forcePathStyle)
	fmt.Printf("      Use SSL: %t\n", useSSL)

	// Test MinIO connection (will fail if MinIO server is not running)
	fmt.Println("\n  🧪 Testing MinIO connection...")
	conn, err := lancedb.Connect(ctx, "s3://test-bucket/lancedb", minioOptions)
	if err != nil {
		fmt.Printf("    ⚠️ MinIO connection failed: %v\n", err)
		fmt.Println("    💡 This is expected if MinIO server is not running locally")
		fmt.Println("    💡 To test MinIO integration:")
		fmt.Println("      1. Start MinIO server: docker run -p 9000:9000 minio/minio server /data")
		fmt.Println("      2. Create bucket 'test-bucket' via MinIO console")
		fmt.Println("      3. Re-run this example")
	} else {
		defer conn.Close()
		fmt.Printf("    ✅ MinIO connection successful!")

		// Test basic operations if connection succeeded
		if err := testStorageConfiguration("MinIO", conn, ctx); err != nil {
			fmt.Printf("    ⚠️ MinIO storage test failed: %v\n", err)
		}
	}

	// MinIO with custom configuration
	fmt.Println("\n  🔹 MinIO with custom configuration")

	customEndpoint := "http://minio.example.com:9000"
	customAccessKey := "custom-access-key"
	customSecretKey := "custom-secret-key"

	_ = &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			S3Config: &lancedb.S3Config{
				Endpoint:        &customEndpoint,
				AccessKeyId:     &customAccessKey,
				SecretAccessKey: &customSecretKey,
				ForcePathStyle:  &forcePathStyle,
				UseSSL:          &useSSL,
			},
		},
	}

	fmt.Printf("    📝 Custom MinIO configuration:\n")
	fmt.Printf("      Endpoint: %s\n", customEndpoint)
	fmt.Printf("      Custom credentials configured\n")

	fmt.Println("\n  💡 MinIO Best Practices:")
	fmt.Println("    • Use MinIO for S3-compatible local development")
	fmt.Println("    • Enable SSL/TLS for production MinIO deployments")
	fmt.Println("    • Set up proper access policies and user management")
	fmt.Println("    • Configure distributed MinIO for high availability")
	fmt.Println("    • Use consistent bucket naming conventions")
	fmt.Println("    • Monitor MinIO performance and storage usage")

	return nil
}

func performStorageComparison(ctx context.Context) error {
	fmt.Println("  ⚡ Storage Performance Comparison")

	// Create test configurations
	configs := make(map[string]struct {
		conn    *lancedb.Connection
		cleanup func()
	})

	// Local storage (basic)
	tempDir1, err := os.MkdirTemp("", "lancedb_perf_basic_")
	if err != nil {
		return fmt.Errorf("failed to create basic temp directory: %w", err)
	}

	conn1, err := lancedb.Connect(ctx, tempDir1, nil)
	if err != nil {
		os.RemoveAll(tempDir1)
		return fmt.Errorf("failed to connect to basic storage: %w", err)
	}

	configs["Local Basic"] = struct {
		conn    *lancedb.Connection
		cleanup func()
	}{
		conn: conn1,
		cleanup: func() {
			conn1.Close()
			os.RemoveAll(tempDir1)
		},
	}

	// Local storage (optimized)
	tempDir2, err := os.MkdirTemp("", "lancedb_perf_optimized_")
	if err != nil {
		configs["Local Basic"].cleanup()
		return fmt.Errorf("failed to create optimized temp directory: %w", err)
	}

	createDirs := true
	useMemoryMap := true
	syncWrites := false

	optimizedOptions := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			LocalConfig: &lancedb.LocalConfig{
				CreateDirIfNotExists: &createDirs,
				UseMemoryMap:         &useMemoryMap,
				SyncWrites:           &syncWrites,
			},
			BlockSize: &[]int{2 * 1024 * 1024}[0], // 2MB blocks
		},
	}

	conn2, err := lancedb.Connect(ctx, tempDir2, optimizedOptions)
	if err != nil {
		configs["Local Basic"].cleanup()
		os.RemoveAll(tempDir2)
		return fmt.Errorf("failed to connect to optimized storage: %w", err)
	}

	configs["Local Optimized"] = struct {
		conn    *lancedb.Connection
		cleanup func()
	}{
		conn: conn2,
		cleanup: func() {
			conn2.Close()
			os.RemoveAll(tempDir2)
		},
	}

	// Cleanup function
	defer func() {
		for _, config := range configs {
			config.cleanup()
		}
	}()

	// Performance test function
	performanceTest := func(name string, conn *lancedb.Connection) (time.Duration, error) {
		start := time.Now()

		// Create table
		table, schema, err := createTestTable(conn, ctx, fmt.Sprintf("perf_test_%s", name))
		if err != nil {
			return 0, err
		}
		defer table.Close()

		// Insert test data
		testData := generateTestData(1000)
		if err := insertTestData(table, schema, testData); err != nil {
			return 0, err
		}

		// Perform some queries
		for i := 0; i < 5; i++ {
			queryVector := generateRandomVector(VectorDim)
			_, err := table.VectorSearch("vector", queryVector, 10)
			if err != nil {
				return 0, err
			}
		}

		return time.Since(start), nil
	}

	// Run performance tests
	fmt.Println("  🏃 Running performance tests...")

	results := make(map[string]time.Duration)

	for name, config := range configs {
		fmt.Printf("  📊 Testing %s...\n", name)

		duration, err := performanceTest(name, config.conn)
		if err != nil {
			fmt.Printf("    ⚠️ Performance test failed: %v\n", err)
			continue
		}

		results[name] = duration
		fmt.Printf("    ⏱️ %s: %v\n", name, duration)
	}

	// Compare results
	if len(results) > 1 {
		fmt.Println("\n  📈 Performance Comparison:")
		var baseline time.Duration
		var baselineName string

		for name, duration := range results {
			if baseline == 0 || duration < baseline {
				baseline = duration
				baselineName = name
			}
		}

		for name, duration := range results {
			if name == baselineName {
				fmt.Printf("    🏆 %s: %v (baseline - fastest)\n", name, duration)
			} else {
				ratio := float64(duration) / float64(baseline)
				fmt.Printf("    📊 %s: %v (%.2fx slower than baseline)\n", name, duration, ratio)
			}
		}
	}

	fmt.Println("\n  💡 Performance Optimization Tips:")
	fmt.Println("    • Use memory mapping for read-heavy workloads")
	fmt.Println("    • Increase block size for better throughput")
	fmt.Println("    • Disable sync writes for better insert performance")
	fmt.Println("    • Consider SSD storage for latency-sensitive applications")
	fmt.Println("    • Monitor I/O patterns and adjust configuration accordingly")

	return nil
}

func demonstrateAdvancedConfigurations(ctx context.Context) error {
	fmt.Println("  🔧 Advanced Storage Configurations")

	// Configuration 1: Multi-tier storage strategy
	fmt.Println("  🔹 Multi-tier storage strategy simulation")

	// Hot storage (fast, expensive)
	hotDir, err := os.MkdirTemp("", "lancedb_hot_")
	if err != nil {
		return fmt.Errorf("failed to create hot storage directory: %w", err)
	}
	defer os.RemoveAll(hotDir)

	hotConfig := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			LocalConfig: &lancedb.LocalConfig{
				UseMemoryMap: &[]bool{true}[0],
				SyncWrites:   &[]bool{false}[0],
			},
			BlockSize: &[]int{4 * 1024 * 1024}[0], // 4MB blocks for throughput
		},
	}

	hotConn, err := lancedb.Connect(ctx, hotDir, hotConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to hot storage: %w", err)
	}
	defer hotConn.Close()

	fmt.Printf("    🔥 Hot storage (SSD-optimized): %s\n", hotDir)

	// Cold storage (slow, cheap)
	coldDir, err := os.MkdirTemp("", "lancedb_cold_")
	if err != nil {
		return fmt.Errorf("failed to create cold storage directory: %w", err)
	}
	defer os.RemoveAll(coldDir)

	coldConfig := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			LocalConfig: &lancedb.LocalConfig{
				UseMemoryMap: &[]bool{false}[0], // Less memory usage
				SyncWrites:   &[]bool{true}[0],  // Ensure durability
			},
			BlockSize: &[]int{1024 * 1024}[0], // Smaller blocks for cold storage
		},
	}

	coldConn, err := lancedb.Connect(ctx, coldDir, coldConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to cold storage: %w", err)
	}
	defer coldConn.Close()

	fmt.Printf("    🧊 Cold storage (HDD-optimized): %s\n", coldDir)

	// Configuration 2: Environment-specific configurations
	fmt.Println("\n  🔹 Environment-specific configurations")

	environments := map[string]*lancedb.ConnectionOptions{
		"Development": {
			StorageOptions: &lancedb.StorageOptions{
				MaxRetries:     &[]int{1}[0], // Fail fast in development
				Timeout:        &[]int{5}[0], // Short timeout
				ConnectTimeout: &[]int{3}[0], // Quick connection timeout
			},
		},
		"Staging": {
			StorageOptions: &lancedb.StorageOptions{
				MaxRetries:     &[]int{3}[0],  // Some resilience
				Timeout:        &[]int{15}[0], // Medium timeout
				ConnectTimeout: &[]int{5}[0],  // Medium connection timeout
			},
		},
		"Production": {
			StorageOptions: &lancedb.StorageOptions{
				MaxRetries:     &[]int{5}[0],               // High resilience
				Timeout:        &[]int{30}[0],              // Longer timeout
				ConnectTimeout: &[]int{10}[0],              // Patient connection
				BlockSize:      &[]int{8 * 1024 * 1024}[0], // Large blocks for production
			},
		},
	}

	for env, config := range environments {
		fmt.Printf("    🌍 %s environment:\n", env)
		fmt.Printf("      Max Retries: %d\n", *config.StorageOptions.MaxRetries)
		fmt.Printf("      Timeout: %d seconds\n", *config.StorageOptions.Timeout)
		fmt.Printf("      Connect Timeout: %d seconds\n", *config.StorageOptions.ConnectTimeout)
		if config.StorageOptions.BlockSize != nil {
			fmt.Printf("      Block Size: %d bytes\n", *config.StorageOptions.BlockSize)
		}
	}

	// Configuration 3: Network-optimized settings
	fmt.Println("\n  🔹 Network-optimized settings for remote storage")
	maxRetries := 5
	timeout := 30
	connectTimeout := 10
	readTimeout := 30
	poolIdleTimeout := 300
	poolMaxIdlePerHost := 10
	networkOptimized := &lancedb.ConnectionOptions{
		StorageOptions: &lancedb.StorageOptions{
			MaxRetries:         &maxRetries,
			Timeout:            &timeout,            // Longer timeout for network operations
			ConnectTimeout:     &connectTimeout,     // Patient connection for network
			ReadTimeout:        &readTimeout,        // Read timeout for slow networks
			PoolIdleTimeout:    &poolIdleTimeout,    // 5 minutes idle timeout
			PoolMaxIdlePerHost: &poolMaxIdlePerHost, // Connection pooling
		},
	}

	fmt.Printf("    🌐 Network-optimized configuration:\n")
	fmt.Printf("      Connection pooling: %d max idle per host\n",
		*networkOptimized.StorageOptions.PoolMaxIdlePerHost)
	fmt.Printf("      Pool idle timeout: %d seconds\n",
		*networkOptimized.StorageOptions.PoolIdleTimeout)
	fmt.Printf("      Read timeout: %d seconds\n",
		*networkOptimized.StorageOptions.ReadTimeout)

	fmt.Println("\n  💡 Advanced Configuration Guidelines:")
	fmt.Println("    • Tailor configurations to your deployment environment")
	fmt.Println("    • Use different settings for development vs production")
	fmt.Println("    • Implement tiered storage for cost optimization")
	fmt.Println("    • Optimize network settings for remote storage")
	fmt.Println("    • Monitor and adjust configurations based on usage patterns")

	return nil
}

func demonstrateErrorHandling(ctx context.Context) error {
	fmt.Println("  🛡️ Error Handling and Fallback Strategies")

	// Strategy 1: Multiple storage backends with fallback
	fmt.Println("  🔹 Multi-backend fallback strategy")

	storageBackends := []struct {
		name   string
		config *lancedb.ConnectionOptions
		uri    string
	}{
		{
			name: "Primary S3",
			uri:  "s3://primary-bucket/lancedb",
			config: &lancedb.ConnectionOptions{
				StorageOptions: &lancedb.StorageOptions{
					S3Config: &lancedb.S3Config{
						Region: &[]string{"us-east-1"}[0],
					},
				},
			},
		},
		{
			name: "Backup S3",
			uri:  "s3://backup-bucket/lancedb",
			config: &lancedb.ConnectionOptions{
				StorageOptions: &lancedb.StorageOptions{
					S3Config: &lancedb.S3Config{
						Region: &[]string{"us-west-2"}[0],
					},
				},
			},
		},
	}

	// Add local fallback
	localDir, err := os.MkdirTemp("", "lancedb_fallback_")
	if err != nil {
		return fmt.Errorf("failed to create fallback directory: %w", err)
	}
	defer os.RemoveAll(localDir)

	storageBackends = append(storageBackends, struct {
		name   string
		config *lancedb.ConnectionOptions
		uri    string
	}{
		name:   "Local Fallback",
		uri:    localDir,
		config: nil, // Basic local configuration
	})

	// Try each backend in order
	var conn *lancedb.Connection
	var successfulBackend string

	for _, backend := range storageBackends {
		fmt.Printf("    🔄 Trying %s...\n", backend.name)

		c, err := lancedb.Connect(ctx, backend.uri, backend.config)
		if err != nil {
			fmt.Printf("      ❌ %s failed: %v\n", backend.name, err)
			continue
		}

		// Test the connection
		if testConn, testErr := testConnection(c, ctx); testErr != nil {
			fmt.Printf("      ❌ %s connection test failed: %v\n", backend.name, testErr)
			c.Close()
			continue
		} else {
			testConn.Close()
		}

		conn = c
		successfulBackend = backend.name
		fmt.Printf("      ✅ %s succeeded\n", backend.name)
		break
	}

	if conn == nil {
		fmt.Println("    ❌ All storage backends failed")
	} else {
		fmt.Printf("    🎉 Using %s as active storage backend\n", successfulBackend)
		conn.Close()
	}

	// Strategy 2: Retry with exponential backoff
	fmt.Println("\n  🔹 Retry with exponential backoff")

	retryWithBackoff := func(maxRetries int, baseDelay time.Duration) error {
		for attempt := 1; attempt <= maxRetries; attempt++ {
			fmt.Printf("    🔄 Connection attempt %d/%d...\n", attempt, maxRetries)

			// Simulate connection attempt (will fail for S3 without credentials)
			_, err := lancedb.Connect(ctx, "s3://test-bucket/lancedb", &lancedb.ConnectionOptions{
				StorageOptions: &lancedb.StorageOptions{
					S3Config: &lancedb.S3Config{
						Region: &[]string{"us-east-1"}[0],
					},
				},
			})

			if err == nil {
				fmt.Println("      ✅ Connection successful")
				return nil
			}

			fmt.Printf("      ❌ Attempt %d failed: %v\n", attempt, err)

			if attempt < maxRetries {
				// Exponential backoff: 1s, 2s, 4s, 8s, etc.
				delay := baseDelay * time.Duration(1<<uint(attempt-1))
				fmt.Printf("      ⏳ Waiting %v before next attempt...\n", delay)
				time.Sleep(delay)
			}
		}

		return fmt.Errorf("failed after %d attempts", maxRetries)
	}

	err = retryWithBackoff(4, 1*time.Second)
	if err != nil {
		fmt.Printf("    💡 Retry strategy completed (expected failure without real S3): %v\n", err)
	}

	// Strategy 3: Circuit breaker pattern
	fmt.Println("\n  🔹 Circuit breaker pattern simulation")

	type CircuitBreaker struct {
		failures    int
		maxFailures int
		isOpen      bool
		lastFailure time.Time
		timeout     time.Duration
	}

	circuit := &CircuitBreaker{
		maxFailures: 3,
		timeout:     5 * time.Second,
	}

	attemptConnection := func() error {
		// Check if circuit is open
		if circuit.isOpen {
			if time.Since(circuit.lastFailure) < circuit.timeout {
				return fmt.Errorf("circuit breaker is open, last failure: %v", circuit.lastFailure)
			}
			// Try to close the circuit
			fmt.Println("      🔄 Circuit breaker timeout elapsed, trying to close...")
			circuit.isOpen = false
			circuit.failures = 0
		}

		// Simulate connection attempt
		_, err := lancedb.Connect(ctx, "s3://test-bucket/lancedb", nil)

		if err != nil {
			circuit.failures++
			circuit.lastFailure = time.Now()

			if circuit.failures >= circuit.maxFailures {
				circuit.isOpen = true
				fmt.Printf("      ⚠️ Circuit breaker opened after %d failures\n", circuit.failures)
			}

			return fmt.Errorf("connection failed (failure %d/%d): %v",
				circuit.failures, circuit.maxFailures, err)
		}

		// Success - reset circuit breaker
		circuit.failures = 0
		circuit.isOpen = false
		return nil
	}

	// Simulate multiple attempts
	for i := 1; i <= 6; i++ {
		fmt.Printf("    🔄 Circuit breaker attempt %d...\n", i)
		err := attemptConnection()
		if err != nil {
			fmt.Printf("      ❌ %v\n", err)
		} else {
			fmt.Printf("      ✅ Connection successful\n")
			break
		}

		if i == 4 {
			// Simulate timeout passing
			fmt.Printf("      ⏳ Simulating timeout passage...\n")
			circuit.lastFailure = time.Now().Add(-6 * time.Second)
		}

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n  💡 Error Handling Best Practices:")
	fmt.Println("    • Implement multiple storage backend fallbacks")
	fmt.Println("    • Use exponential backoff for transient failures")
	fmt.Println("    • Implement circuit breakers for persistent failures")
	fmt.Println("    • Log detailed error information for debugging")
	fmt.Println("    • Monitor error rates and alert on anomalies")
	fmt.Println("    • Test failure scenarios in development")

	return nil
}

// Helper functions

func testStorageConfiguration(name string, conn *lancedb.Connection, ctx context.Context) error {
	// Create a simple table and test basic operations
	table, schema, err := createTestTable(conn, ctx, fmt.Sprintf("test_%s", name))
	if err != nil {
		return err
	}
	defer table.Close()

	// Insert sample data
	testData := generateTestData(100)
	if err := insertTestData(table, schema, testData); err != nil {
		return err
	}

	// Perform a simple query
	queryVector := generateRandomVector(VectorDim)
	results, err := table.VectorSearch("vector", queryVector, 5)
	if err != nil {
		return err
	}

	fmt.Printf("    ✅ %s storage test completed: %d records inserted, %d results from query\n",
		name, len(testData), len(results))

	return nil
}

func createTestTable(conn *lancedb.Connection, ctx context.Context, tableName string) (*lancedb.Table, *arrow.Schema, error) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "vector", Type: arrow.FixedSizeListOf(VectorDim, arrow.PrimitiveTypes.Float32), Nullable: false},
	}

	arrowSchema := arrow.NewSchema(fields, nil)
	schema, err := lancedb.NewSchema(arrowSchema)
	if err != nil {
		return nil, nil, err
	}

	table, err := conn.CreateTable(ctx, tableName, *schema)
	if err != nil {
		return nil, nil, err
	}
	return table, arrowSchema, nil
}

func generateTestData(count int) []StorageTestRecord {
	records := make([]StorageTestRecord, count)

	for i := 0; i < count; i++ {
		records[i] = StorageTestRecord{
			ID:     int32(i + 1),
			Name:   fmt.Sprintf("Test Record %d", i+1),
			Vector: generateRandomVector(VectorDim),
		}
	}

	return records
}

func generateRandomVector(dimensions int) []float32 {
	vector := make([]float32, dimensions)
	for i := 0; i < dimensions; i++ {
		vector[i] = rand.Float32()*2 - 1 // Random values between -1 and 1
	}

	// Normalize vector
	var norm float32
	for _, v := range vector {
		norm += v * v
	}
	norm = float32(math.Sqrt(float64(norm)))

	if norm > 0 {
		for i := range vector {
			vector[i] /= norm
		}
	}

	return vector
}

func insertTestData(table *lancedb.Table, schema *arrow.Schema, data []StorageTestRecord) error {
	pool := memory.NewGoAllocator()

	// Prepare data
	ids := make([]int32, len(data))
	names := make([]string, len(data))
	allVectors := make([]float32, len(data)*VectorDim)

	for i, record := range data {
		ids[i] = record.ID
		names[i] = record.Name
		copy(allVectors[i*VectorDim:(i+1)*VectorDim], record.Vector)
	}

	// Build arrays
	idBuilder := array.NewInt32Builder(pool)
	idBuilder.AppendValues(ids, nil)
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	nameBuilder := array.NewStringBuilder(pool)
	nameBuilder.AppendValues(names, nil)
	nameArray := nameBuilder.NewArray()
	defer nameArray.Release()

	// Vector array
	vectorBuilder := array.NewFloat32Builder(pool)
	vectorBuilder.AppendValues(allVectors, nil)
	vectorFloat32Array := vectorBuilder.NewArray()
	defer vectorFloat32Array.Release()

	vectorListType := arrow.FixedSizeListOf(VectorDim, arrow.PrimitiveTypes.Float32)
	vectorArray := array.NewFixedSizeListData(
		array.NewData(vectorListType, len(data), []*memory.Buffer{nil},
			[]arrow.ArrayData{vectorFloat32Array.Data()}, 0, 0),
	)
	defer vectorArray.Release()

	// Create record
	columns := []arrow.Array{idArray, nameArray, vectorArray}
	record := array.NewRecord(schema, columns, int64(len(data)))
	defer record.Release()

	return table.Add(record, nil)
}

func testConnection(conn *lancedb.Connection, ctx context.Context) (*lancedb.Table, error) {
	// Try to create a simple table to test the connection
	table, _, err := createTestTable(conn, ctx, "connection_test")
	if err != nil {
		return nil, err
	}
	return table, nil
}

func printS3Config(name string, config *lancedb.S3Config) {
	fmt.Printf("    📝 %s:\n", name)
	if config.AccessKeyId != nil {
		fmt.Printf("      Access Key: %s...\n", (*config.AccessKeyId)[:min(len(*config.AccessKeyId), 8)])
	}
	if config.Region != nil {
		fmt.Printf("      Region: %s\n", *config.Region)
	}
	if config.Profile != nil {
		fmt.Printf("      Profile: %s\n", *config.Profile)
	}
	if config.AnonymousAccess != nil && *config.AnonymousAccess {
		fmt.Printf("      Anonymous Access: %t\n", *config.AnonymousAccess)
	}
	if config.SessionToken != nil {
		fmt.Printf("      Session Token: configured\n")
	}
	if config.UseSSL != nil {
		fmt.Printf("      Use SSL: %t\n", *config.UseSSL)
	}
	if config.ServerSideEncrypt != nil {
		fmt.Printf("      Server Side Encryption: %s\n", *config.ServerSideEncrypt)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
