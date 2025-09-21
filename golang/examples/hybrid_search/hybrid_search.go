// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// Hybrid Search Example
//
// This example demonstrates combining vector similarity search with traditional
// SQL-like filtering for powerful hybrid search capabilities. It covers:
// - Vector search with metadata filtering
// - Multi-modal search combining semantic and structured queries
// - Advanced filtering with vector similarity
// - Performance optimization for hybrid queries
// - Real-world e-commerce search scenarios

package main

import (
	"context"
	"fmt"
	lancedb "github.com/lancedb/lancedb/pkg"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

const (
	EmbeddingDim = 256
	NumProducts  = 500
)

type Product struct {
	ID          int32
	Name        string
	Description string
	Category    string
	Brand       string
	Price       float64
	Rating      float64
	InStock     bool
	Tags        string
	Vector      []float32
}

func main() {
	fmt.Println("🔀 LanceDB Go SDK - Hybrid Search Example")
	fmt.Println("==================================================")

	// Setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempDir, err := os.MkdirTemp("", "lancedb_hybrid_example_")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)
	fmt.Printf("📂 Using database directory: %s\n", tempDir)

	// Connect and setup
	fmt.Println("\n📋 Step 1: Setting up e-commerce product database...")
	conn, err := lancedb.Connect(ctx, tempDir, nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create product catalog table
	table, schema, err := createProductTable(conn, ctx)
	if err != nil {
		log.Fatalf("Failed to create product table: %v", err)
	}
	defer table.Close()
	fmt.Printf("✅ Created product catalog with %d-dimensional embeddings\n", EmbeddingDim)

	// Insert sample products
	fmt.Println("\n📋 Step 2: Loading product catalog...")
	if err := insertProductCatalog(table, schema); err != nil {
		log.Fatalf("Failed to insert products: %v", err)
	}
	fmt.Printf("✅ Loaded %d products into catalog\n", NumProducts)

	// Demonstrate different hybrid search patterns
	fmt.Println("\n📋 Step 3: Basic hybrid search (semantic + filters)...")
	if err := basicHybridSearch(table); err != nil {
		log.Fatalf("Failed basic hybrid search: %v", err)
	}

	fmt.Println("\n📋 Step 4: E-commerce search scenarios...")
	if err := ecommerceSearchScenarios(table); err != nil {
		log.Fatalf("Failed e-commerce scenarios: %v", err)
	}

	fmt.Println("\n📋 Step 5: Advanced multi-modal queries...")
	if err := advancedMultiModalQueries(table); err != nil {
		log.Fatalf("Failed advanced queries: %v", err)
	}

	fmt.Println("\n📋 Step 6: Performance comparison...")
	if err := performanceComparison(table); err != nil {
		log.Fatalf("Failed performance comparison: %v", err)
	}

	fmt.Println("\n📋 Step 7: Recommendation system patterns...")
	if err := recommendationPatterns(table); err != nil {
		log.Fatalf("Failed recommendation patterns: %v", err)
	}

	fmt.Println("\n🎉 Hybrid search examples completed successfully!")
	fmt.Println("==================================================")
}

func createProductTable(conn *lancedb.Connection, ctx context.Context) (*lancedb.Table, *arrow.Schema, error) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "description", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "brand", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "rating", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "in_stock", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "tags", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "vector", Type: arrow.FixedSizeListOf(EmbeddingDim, arrow.PrimitiveTypes.Float32), Nullable: false},
	}

	arrowSchema := arrow.NewSchema(fields, nil)
	schema, err := lancedb.NewSchema(arrowSchema)
	if err != nil {
		return nil, nil, err
	}

	table, err := conn.CreateTable(ctx, "products", *schema)
	return table, arrowSchema, nil
}

func insertProductCatalog(table *lancedb.Table, schema *arrow.Schema) error {
	pool := memory.NewGoAllocator()
	rand.Seed(time.Now().UnixNano())

	products := generateProductCatalog()

	// Create Arrow arrays
	ids := make([]int32, len(products))
	names := make([]string, len(products))
	descriptions := make([]string, len(products))
	categories := make([]string, len(products))
	brands := make([]string, len(products))
	prices := make([]float64, len(products))
	ratings := make([]float64, len(products))
	inStock := make([]bool, len(products))
	tags := make([]string, len(products))
	allVectors := make([]float32, len(products)*EmbeddingDim)

	for i, product := range products {
		ids[i] = product.ID
		names[i] = product.Name
		descriptions[i] = product.Description
		categories[i] = product.Category
		brands[i] = product.Brand
		prices[i] = product.Price
		ratings[i] = product.Rating
		inStock[i] = product.InStock
		tags[i] = product.Tags
		copy(allVectors[i*EmbeddingDim:(i+1)*EmbeddingDim], product.Vector)
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

	descBuilder := array.NewStringBuilder(pool)
	descBuilder.AppendValues(descriptions, nil)
	descArray := descBuilder.NewArray()
	defer descArray.Release()

	catBuilder := array.NewStringBuilder(pool)
	catBuilder.AppendValues(categories, nil)
	catArray := catBuilder.NewArray()
	defer catArray.Release()

	brandBuilder := array.NewStringBuilder(pool)
	brandBuilder.AppendValues(brands, nil)
	brandArray := brandBuilder.NewArray()
	defer brandArray.Release()

	priceBuilder := array.NewFloat64Builder(pool)
	priceBuilder.AppendValues(prices, nil)
	priceArray := priceBuilder.NewArray()
	defer priceArray.Release()

	ratingBuilder := array.NewFloat64Builder(pool)
	ratingBuilder.AppendValues(ratings, nil)
	ratingArray := ratingBuilder.NewArray()
	defer ratingArray.Release()

	stockBuilder := array.NewBooleanBuilder(pool)
	stockBuilder.AppendValues(inStock, nil)
	stockArray := stockBuilder.NewArray()
	defer stockArray.Release()

	tagsBuilder := array.NewStringBuilder(pool)
	tagsBuilder.AppendValues(tags, nil)
	tagsArray := tagsBuilder.NewArray()
	defer tagsArray.Release()

	// Vector array
	vectorBuilder := array.NewFloat32Builder(pool)
	vectorBuilder.AppendValues(allVectors, nil)
	vectorFloat32Array := vectorBuilder.NewArray()
	defer vectorFloat32Array.Release()

	vectorListType := arrow.FixedSizeListOf(EmbeddingDim, arrow.PrimitiveTypes.Float32)
	vectorArray := array.NewFixedSizeListData(
		array.NewData(vectorListType, len(products), []*memory.Buffer{nil},
			[]arrow.ArrayData{vectorFloat32Array.Data()}, 0, 0),
	)
	defer vectorArray.Release()

	// Create and insert record
	columns := []arrow.Array{idArray, nameArray, descArray, catArray, brandArray,
		priceArray, ratingArray, stockArray, tagsArray, vectorArray}
	record := array.NewRecord(schema, columns, int64(len(products)))
	defer record.Release()

	return table.Add(record, nil)
}

func generateProductCatalog() []Product {
	categories := []string{"Electronics", "Clothing", "Home", "Books", "Sports", "Beauty"}
	brands := map[string][]string{
		"Electronics": {"Apple", "Samsung", "Sony", "Dell", "HP", "Canon"},
		"Clothing":    {"Nike", "Adidas", "Zara", "H&M", "Levi's", "Gucci"},
		"Home":        {"IKEA", "Wayfair", "CB2", "West Elm", "Pottery Barn", "Target"},
		"Books":       {"Penguin", "HarperCollins", "Random House", "Simon & Schuster", "Macmillan", "Hachette"},
		"Sports":      {"Nike", "Adidas", "Under Armour", "Puma", "Reebok", "New Balance"},
		"Beauty":      {"L'Oreal", "Maybelline", "MAC", "Clinique", "Estee Lauder", "Sephora"},
	}

	products := make([]Product, 0, NumProducts)

	for i := 0; i < NumProducts; i++ {
		category := categories[rand.Intn(len(categories))]
		brandList := brands[category]
		brand := brandList[rand.Intn(len(brandList))]

		name, description := generateProductInfo(category, brand, i)

		product := Product{
			ID:          int32(i + 1),
			Name:        name,
			Description: description,
			Category:    category,
			Brand:       brand,
			Price:       generatePrice(category),
			Rating:      2.0 + rand.Float64()*3.0, // 2.0 - 5.0
			InStock:     rand.Float64() > 0.1,     // 90% in stock
			Tags:        generateTags(category, brand),
			Vector:      generateProductEmbedding(name, description, category, brand),
		}

		products = append(products, product)
	}

	return products
}

func generateProductInfo(category, brand string, index int) (string, string) {
	nameTemplates := map[string][]string{
		"Electronics": {
			"%s Smartphone Pro %d", "%s Laptop Ultra %d", "%s Camera EOS %d",
			"%s Tablet Air %d", "%s Headphones Max %d", "%s TV Smart %d",
		},
		"Clothing": {
			"%s Running Shoes %d", "%s Casual Jacket %d", "%s Denim Jeans %d",
			"%s Cotton T-Shirt %d", "%s Winter Coat %d", "%s Summer Dress %d",
		},
		"Home": {
			"%s Dining Table %d", "%s Sofa Sectional %d", "%s Bed Frame %d",
			"%s Office Chair %d", "%s Kitchen Set %d", "%s Bookshelf %d",
		},
		"Books": {
			"%s Novel Series %d", "%s Cookbook %d", "%s Science Guide %d",
			"%s History Book %d", "%s Art Collection %d", "%s Biography %d",
		},
		"Sports": {
			"%s Basketball Shoes %d", "%s Yoga Mat %d", "%s Fitness Tracker %d",
			"%s Tennis Racket %d", "%s Swimming Goggles %d", "%s Running Gear %d",
		},
		"Beauty": {
			"%s Lipstick %d", "%s Foundation %d", "%s Skincare Set %d",
			"%s Perfume %d", "%s Nail Polish %d", "%s Eye Shadow %d",
		},
	}

	descTemplates := map[string]string{
		"Electronics": "High-quality %s device with advanced features and premium build quality. Perfect for professional and personal use.",
		"Clothing":    "Stylish and comfortable %s apparel made from premium materials. Perfect for any occasion with modern design.",
		"Home":        "Beautiful %s furniture piece that combines functionality with elegant design. Perfect for modern homes.",
		"Books":       "Engaging and informative %s publication that provides valuable insights and entertainment for readers.",
		"Sports":      "Professional-grade %s equipment designed for performance and durability. Perfect for athletes and fitness enthusiasts.",
		"Beauty":      "Premium %s cosmetic product with long-lasting formula and beautiful finish. Perfect for daily use.",
	}

	templates := nameTemplates[category]
	template := templates[rand.Intn(len(templates))]
	name := fmt.Sprintf(template, brand, index%100+1)

	descTemplate := descTemplates[category]
	description := fmt.Sprintf(descTemplate, category)

	return name, description
}

func generatePrice(category string) float64 {
	basePrices := map[string][2]float64{
		"Electronics": {50.0, 2000.0},
		"Clothing":    {20.0, 500.0},
		"Home":        {100.0, 3000.0},
		"Books":       {10.0, 50.0},
		"Sports":      {25.0, 800.0},
		"Beauty":      {15.0, 200.0},
	}

	priceRange := basePrices[category]
	return priceRange[0] + rand.Float64()*(priceRange[1]-priceRange[0])
}

func generateTags(category, brand string) string {
	commonTags := []string{"popular", "trending", "bestseller", "new", "featured"}
	categoryTags := map[string][]string{
		"Electronics": {"tech", "gadget", "digital", "smart", "wireless"},
		"Clothing":    {"fashion", "style", "casual", "formal", "trendy"},
		"Home":        {"furniture", "decor", "modern", "vintage", "comfort"},
		"Books":       {"literature", "educational", "fiction", "non-fiction", "bestseller"},
		"Sports":      {"fitness", "athletic", "outdoor", "training", "professional"},
		"Beauty":      {"cosmetic", "skincare", "makeup", "beauty", "luxury"},
	}

	var tags []string
	tags = append(tags, commonTags[rand.Intn(len(commonTags))])
	tags = append(tags, categoryTags[category][rand.Intn(len(categoryTags[category]))])
	tags = append(tags, fmt.Sprintf("brand_%s", brand))

	return fmt.Sprintf("%s,%s,%s", tags[0], tags[1], tags[2])
}

func generateProductEmbedding(name, description, category, brand string) []float32 {
	vector := make([]float32, EmbeddingDim)

	// Create deterministic embedding based on product features
	seed := hash(name + description + category + brand)
	rand.Seed(int64(seed))

	// Base random embedding
	for i := 0; i < EmbeddingDim; i++ {
		vector[i] = float32(rand.NormFloat64() * 0.1)
	}

	// Category clustering
	categoryHash := hash(category) % uint32(EmbeddingDim/8)
	for i := 0; i < EmbeddingDim/8; i++ {
		vector[int(categoryHash)*8+i] += 0.3
	}

	// Brand similarity
	brandHash := hash(brand) % uint32(EmbeddingDim/16)
	for i := 0; i < EmbeddingDim/16; i++ {
		vector[int(brandHash)*16+i] += 0.2
	}

	// Normalize
	normalizeVector(vector)
	return vector
}

func hash(s string) uint32 {
	h := uint32(2166136261)
	for _, c := range s {
		h ^= uint32(c)
		h *= 16777619
	}
	return h
}

func normalizeVector(vector []float32) {
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
}

func basicHybridSearch(table *lancedb.Table) error {
	fmt.Println("  🔍 Basic hybrid search: Vector similarity + metadata filters")

	// Search for "smartphone" with price and availability filters
	queryVector := generateProductEmbedding("smartphone", "mobile phone device", "Electronics", "Apple")

	// Vector search with price filter
	fmt.Println("  📱 Searching for smartphones under $1000 that are in stock...")
	results, err := table.VectorSearchWithFilter("vector", queryVector, 5,
		"price < 1000 AND in_stock = true")
	if err != nil {
		return fmt.Errorf("hybrid search failed: %w", err)
	}

	fmt.Printf("  📊 Found %d matching products:\n", len(results))
	for i, result := range results {
		fmt.Printf("    %d. %v\n", i+1, result["name"])
		fmt.Printf("       Price: $%.2f, Rating: %.1f, In Stock: %v\n",
			result["price"], result["rating"], result["in_stock"])
		fmt.Printf("       Similarity: %.4f\n\n", 1.0-result["_distance"].(float64))
	}

	return nil
}

func ecommerceSearchScenarios(table *lancedb.Table) error {
	fmt.Println("  🛒 E-commerce search scenarios")

	// Scenario 1: Budget-conscious shopper
	fmt.Println("  💰 Scenario 1: Budget shopper looking for running shoes under $100")
	queryVector := generateProductEmbedding("running shoes", "athletic footwear sports", "Sports", "Nike")

	results, err := table.VectorSearchWithFilter("vector", queryVector, 5,
		"price < 100 AND category = 'Sports' AND rating >= 4.0")
	if err != nil {
		return fmt.Errorf("budget search failed: %w", err)
	}

	fmt.Printf("  📊 Budget options found: %d\n", len(results))
	for i, result := range results {
		fmt.Printf("    %d. %v - $%.2f (Rating: %.1f)\n",
			i+1, result["name"], result["price"], result["rating"])
	}

	// Scenario 2: Brand-specific search
	fmt.Println("\n  👕 Scenario 2: Looking for Nike clothing items, any price")
	queryVector = generateProductEmbedding("clothing apparel", "fashion wear style", "Clothing", "Nike")

	results, err = table.VectorSearchWithFilter("vector", queryVector, 5,
		"brand = 'Nike' AND category = 'Clothing'")
	if err != nil {
		return fmt.Errorf("brand search failed: %w", err)
	}

	fmt.Printf("  📊 Nike clothing found: %d\n", len(results))
	for i, result := range results {
		fmt.Printf("    %d. %v - $%.2f\n", i+1, result["name"], result["price"])
	}

	// Scenario 3: High-end products with high ratings
	fmt.Println("\n  💎 Scenario 3: Premium electronics with excellent ratings")
	queryVector = generateProductEmbedding("premium electronics", "high-end technology device", "Electronics", "Apple")

	results, err = table.VectorSearchWithFilter("vector", queryVector, 5,
		"price > 500 AND rating >= 4.5 AND category = 'Electronics'")
	if err != nil {
		return fmt.Errorf("premium search failed: %w", err)
	}

	fmt.Printf("  📊 Premium electronics found: %d\n", len(results))
	for i, result := range results {
		fmt.Printf("    %d. %v - $%.2f (Rating: %.1f)\n",
			i+1, result["name"], result["price"], result["rating"])
	}

	return nil
}

func advancedMultiModalQueries(table *lancedb.Table) error {
	fmt.Println("  🧠 Advanced multi-modal queries")

	// Complex query with multiple conditions
	fmt.Println("  🎯 Multi-criteria search with vector similarity")
	queryVector := generateProductEmbedding("home furniture", "comfortable living room", "Home", "IKEA")

	limit := 3
	config := lancedb.QueryConfig{
		Columns: []string{"id", "name", "brand", "price", "rating", "category"},
		Where:   "price BETWEEN 200 AND 1000 AND rating >= 4.0 AND in_stock = true",
		Limit:   &limit,
		VectorSearch: &lancedb.VectorSearch{
			Column: "vector",
			Vector: queryVector,
			K:      15, // Get more candidates, then filter
		},
	}

	results, err := table.Select(config)
	if err != nil {
		return fmt.Errorf("multi-modal query failed: %w", err)
	}

	fmt.Printf("  📊 Multi-criteria results: %d\n", len(results))
	for i, result := range results {
		fmt.Printf("    %d. %v (%v)\n", i+1, result["name"], result["brand"])
		fmt.Printf("       $%.2f, Rating: %.1f, Similarity: %.4f\n",
			result["price"], result["rating"], 1.0-result["_distance"].(float64))
	}

	// Tag-based semantic search
	fmt.Println("\n  🏷️ Tag-based semantic search")
	queryVector = generateProductEmbedding("trending fashion", "popular style wear", "Clothing", "Zara")

	results, err = table.VectorSearchWithFilter("vector", queryVector, 5,
		"tags LIKE '%trending%' OR tags LIKE '%popular%'")
	if err != nil {
		return fmt.Errorf("tag-based search failed: %w", err)
	}

	fmt.Printf("  📊 Trending items found: %d\n", len(results))
	for i, result := range results {
		fmt.Printf("    %d. %v - %v\n", i+1, result["name"], result["tags"])
	}

	return nil
}

func performanceComparison(table *lancedb.Table) error {
	fmt.Println("  ⚡ Performance comparison: Vector vs Hybrid vs Traditional")

	queryVector := generateProductEmbedding("smartphone", "mobile device", "Electronics", "Samsung")

	// Pure vector search
	start := time.Now()
	vectorResults, err := table.VectorSearch("vector", queryVector, 10)
	vectorTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("vector search failed: %w", err)
	}

	// Hybrid search
	start = time.Now()
	hybridResults, err := table.VectorSearchWithFilter("vector", queryVector, 10,
		"price < 800 AND rating >= 4.0")
	hybridTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("hybrid search failed: %w", err)
	}

	// Traditional filter search
	start = time.Now()
	traditionalResults, err := table.SelectWithFilter(
		"category = 'Electronics' AND price < 800 AND rating >= 4.0")
	traditionalTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("traditional search failed: %w", err)
	}

	fmt.Printf("  📊 Performance Results:\n")
	fmt.Printf("    Vector Search:      %v (%d results)\n", vectorTime, len(vectorResults))
	fmt.Printf("    Hybrid Search:      %v (%d results)\n", hybridTime, len(hybridResults))
	fmt.Printf("    Traditional Filter: %v (%d results)\n", traditionalTime, len(traditionalResults))

	return nil
}

func recommendationPatterns(table *lancedb.Table) error {
	fmt.Println("  🎯 Recommendation system patterns")

	// "Customers who bought this also bought" pattern
	fmt.Println("  🛍️ Similar product recommendations")

	// Simulate a user's purchase (ID = 100)
	purchasedResults, err := table.SelectWithFilter("id = 100")
	if err != nil {
		return fmt.Errorf("failed to get purchased item: %w", err)
	}

	if len(purchasedResults) == 0 {
		fmt.Println("  ⚠️ No product found with ID 100")
		return nil
	}

	purchasedItem := purchasedResults[0]
	fmt.Printf("  🛒 User purchased: %v\n", purchasedItem["name"])
	fmt.Printf("      Category: %v, Brand: %v, Price: $%.2f\n",
		purchasedItem["category"], purchasedItem["brand"], purchasedItem["price"])

	// Get the vector for the purchased item (this would normally come from your vector store)
	// For this demo, we'll regenerate it based on the product info
	queryVector := generateProductEmbedding(
		purchasedItem["name"].(string),
		purchasedItem["description"].(string),
		purchasedItem["category"].(string),
		purchasedItem["brand"].(string),
	)

	// Find similar products (excluding the purchased item and out-of-stock items)
	recommendations, err := table.VectorSearchWithFilter("vector", queryVector, 5,
		fmt.Sprintf("id != %v AND in_stock = true", purchasedItem["id"]))
	if err != nil {
		return fmt.Errorf("recommendation search failed: %w", err)
	}

	fmt.Printf("\n  💡 Recommended similar products:\n")
	for i, rec := range recommendations {
		fmt.Printf("    %d. %v\n", i+1, rec["name"])
		fmt.Printf("       %v - $%.2f (Similarity: %.3f)\n",
			rec["category"], rec["price"], 1.0-rec["_distance"].(float64))
	}

	// Cross-category recommendations
	fmt.Println("\n  🔄 Cross-category recommendations")
	crossCatResults, err := table.VectorSearchWithFilter("vector", queryVector, 3,
		fmt.Sprintf("category != '%v' AND in_stock = true", purchasedItem["category"]))
	if err != nil {
		return fmt.Errorf("cross-category search failed: %w", err)
	}

	fmt.Printf("  💡 You might also like (other categories):\n")
	for i, rec := range crossCatResults {
		fmt.Printf("    %d. %v (%v)\n", i+1, rec["name"], rec["category"])
		fmt.Printf("       $%.2f (Similarity: %.3f)\n",
			rec["price"], 1.0-rec["_distance"].(float64))
	}

	return nil
}
