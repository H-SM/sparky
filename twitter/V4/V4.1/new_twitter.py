import os
import time
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import gc  # For explicit garbage collection

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, array, lit, monotonically_increasing_id
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors

# GraphFrames and GraphX require additional imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from graphframes import GraphFrame

# PySparkSQL for SQL queries
from pyspark.sql import Row
from pyspark.sql.functions import udf, struct

# For GraphX API access
from py4j.java_gateway import java_import

class OutputLogger:
    def __init__(self, filename='analysis_output.txt'):
        """
        Initialize output logger that captures print statements and writes to file
        
        Parameters:
        filename (str): Output file to store analysis results
        """
        self.terminal = sys.stdout
        self.log = open(filename, "w")
        self.buffer = []
    
    def write(self, message):
        """
        Write message to both terminal and log file
        
        Parameters:
        message (str): Message to log
        """
        self.terminal.write(message)
        self.log.write(message)
        self.buffer.append(message)
    
    def flush(self):
        """
        Flush the buffer and file
        """
        self.terminal.flush()
        self.log.flush()
    
    def close(self):
        """
        Close the log file
        """
        self.log.close()

def create_spark_session():
    """
    Create a Spark session with optimized configurations for M3 MacBook Air (16GB)
    """
    # Create a temporary directory for checkpointing
    checkpoint_dir = "/tmp/spark_checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    # Optimized for MacBook Air M3 with 16GB RAM
    spark = (SparkSession.builder
        .appName("SocialGraphAnalysis")
        .master("local[*]")
        # Conservative memory settings for 16GB total RAM
        .config("spark.executor.memory", "8g")       # Leave room for OS and other processes
        .config("spark.driver.memory", "8g")
        .config("spark.driver.maxResultSize", "4g")  # Limit result size to prevent OOM
        # M3 optimizations
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops -Djdk.nio.maxCachedBufferSize=1024000")
        # Better memory management
        .config("spark.memory.fraction", "0.7")      # More conservative
        .config("spark.memory.storageFraction", "0.3") # More storage for caching
        # Tuned partitioning - more partitions to handle memory better
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.default.parallelism", "8")    # M3 has 8 cores
        # Kryo serializer for better memory efficiency
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # GraphFrames package
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12")
        .getOrCreate())
    
    # Set checkpoint directory
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    # Import GraphX classes
    java_import(spark._jvm, 'org.apache.spark.graphx._')
    java_import(spark._jvm, 'org.apache.spark.graphx.lib._')
    
    return spark

def load_edge_dataset(spark, file_path, sample_fraction=1.0):
    """
    Load edge dataset and create vertex and edge DataFrames
    
    Parameters:
    spark: SparkSession
    file_path: Path to edge list file
    sample_fraction: Fraction of data to sample (0.0-1.0)
    """
    print(f"\n--- Loading Edge Dataset (Sample: {sample_fraction*100}%) ---")
    
    # Define schema for edge dataset
    schema = StructType([
        StructField("src", StringType(), True),
        StructField("dst", StringType(), True)
    ])
    
    # Read edge list
    try:
        edges_df = spark.read.schema(schema).csv(file_path, sep=" ")
        
        # Sample if fraction is less than 1
        if sample_fraction < 1.0:
            print(f"Sampling {sample_fraction*100}% of the original dataset")
            edges_df = edges_df.sample(fraction=sample_fraction, seed=42)
        
        # Remove any duplicate edges to reduce size
        edges_df = edges_df.distinct()
        
        # Cache the smaller DataFrame to improve performance
        edges_df.cache()
        edge_count = edges_df.count()
        print(f"Loaded {edge_count} unique edges")
        
        # Create unique vertex DataFrame
        vertices_df = edges_df.select(col("src").alias("id")).union(
            edges_df.select(col("dst").alias("id"))
        ).distinct()
        
        # Add a monotonically increasing ID as a property to help with graph processing
        vertices_df = vertices_df.withColumn("index", monotonically_increasing_id())
        vertices_df.cache()
        vertex_count = vertices_df.count()
        print(f"Found {vertex_count} unique vertices")
        
        return vertices_df, edges_df
    
    except Exception as e:
        print(f"Error loading dataset: {e}")
        raise e

def graph_basic_analysis(spark, graph):
    """
    Perform basic graph analysis including detailed degree distribution
    """
    print("\n--- Graph Basic Analysis ---")
    print(f"Total Nodes: {graph.vertices.count()}")
    print(f"Total Edges: {graph.edges.count()}")
    
    # Degree Analysis
    print("\nCalculating degree distribution...")
    degrees = graph.degrees
    degrees.createOrReplaceTempView("node_degrees")
    degrees.cache()  # Cache to improve performance
    
    print("\n--- Top 10 Most Connected Nodes ---")
    top_connected_nodes = spark.sql("""
        SELECT id, degree 
        FROM node_degrees 
        ORDER BY degree DESC 
        LIMIT 10
    """)
    top_connected_nodes.show(truncate=False)
    
    # Detailed degree distribution analysis
    print("\n--- Degree Distribution Analysis ---")
    
    degree_stats = spark.sql("""
        SELECT
            MIN(degree) as min_degree,
            MAX(degree) as max_degree,
            AVG(degree) as avg_degree,
            PERCENTILE(degree, 0.5) as median_degree,
            STDDEV(degree) as stddev_degree
        FROM node_degrees
    """)
    
    print("Degree Distribution Statistics:")
    degree_stats.show(truncate=False)
    
    # Histogram of degree distribution (SQL)
    degree_buckets = spark.sql("""
        SELECT 
            CASE
                WHEN degree BETWEEN 1 AND 5 THEN '1-5'
                WHEN degree BETWEEN 6 AND 10 THEN '6-10'
                WHEN degree BETWEEN 11 AND 50 THEN '11-50'
                WHEN degree BETWEEN 51 AND 100 THEN '51-100'
                WHEN degree BETWEEN 101 AND 500 THEN '101-500'
                WHEN degree > 500 THEN '500+'
                ELSE 'Unknown'
            END as degree_range,
            COUNT(*) as node_count
        FROM node_degrees
        GROUP BY degree_range
        ORDER BY degree_range
    """)
    
    print("Degree Distribution by Range:")
    degree_buckets.show(truncate=False)
    
    return degrees

def pagerank_analysis(spark, graph, max_iterations=[5, 10]):
    """
    Perform PageRank analysis with different iterations using GraphFrames
    Reduced iterations to prevent memory issues
    """
    print("\n--- GraphFrames PageRank Analysis ---")
    
    pagerank_results = {}
    for iterations in max_iterations:
        try:
            print(f"\nRunning PageRank with {iterations} iterations...")
            # Force checkpoint before PageRank
            graph.vertices.checkpoint()
            graph.edges.checkpoint()
            spark.catalog.clearCache()  # Clear cache to free memory
            gc.collect()  # Force garbage collection
            
            # Run PageRank with reduced tolerance to converge faster
            pr = graph.pageRank(resetProbability=0.15, maxIter=iterations, tol=0.01)
            
            # Cache and show results
            pr.vertices.cache()
            pagerank_results[iterations] = pr.vertices
            
            print(f"\nPageRank Results (Iterations: {iterations})")
            pr.vertices.orderBy(desc("pagerank")).limit(10).show(truncate=False)
            
            # Uncache to free memory
            pr.vertices.unpersist()
        except Exception as e:
            print(f"Error in PageRank for {iterations} iterations: {e}")
    
    return pagerank_results

def community_detection_safe(spark, graph, max_iterations=5):
    """
    Detect Communities using Connected Components with memory-safe approach
    """
    print("\n--- Community Detection (Memory-Safe Approach) ---")
    
    try:
        # Explicitly checkpoint and clear cache before heavy computation
        graph.vertices.checkpoint()
        graph.edges.checkpoint()
        spark.catalog.clearCache()
        gc.collect()  # Force garbage collection
        
        print("\nRunning Connected Components algorithm...")
        # Run Connected Components with limited iterations to prevent memory issues
        cc = graph.connectedComponents(maxIter=max_iterations)
        cc.cache()
        
        # Count and analyze components
        cc.createOrReplaceTempView("communities")
        
        component_sizes = spark.sql("""
            SELECT component, COUNT(*) as component_size
            FROM (
                SELECT id, component 
                FROM communities
            ) 
            GROUP BY component 
            ORDER BY component_size DESC
            LIMIT 20  -- Limiting to top 20 to reduce memory pressure
        """)
        
        print("Top Largest Community Sizes:")
        component_sizes.show(truncate=False)
        
        # Community statistics
        print("\nCommunity Statistics:")
        community_stats = spark.sql("""
            SELECT 
                COUNT(DISTINCT component) as num_communities,
                AVG(component_size) as avg_community_size,
                MIN(component_size) as min_community_size,
                MAX(component_size) as max_community_size
            FROM (
                SELECT component, COUNT(*) as component_size
                FROM communities
                GROUP BY component
                LIMIT 1000  -- Limiting to reduce memory pressure
            )
        """)
        community_stats.show(truncate=False)
        
        return cc
        
    except Exception as e:
        print(f"Error in Community Detection: {e}")
        import traceback
        traceback.print_exc()
        print("\nSkipping full community detection due to memory constraints.")
        return None

def visualize_degree_distribution(degrees, output_file='degree_distribution_analysis.png'):
    """
    Visualize degree distribution with memory-efficient approach
    """
    print("\nPreparing degree distribution visualization...")
    
    # Convert to pandas but limit the data size
    degree_counts = degrees.groupBy("degree").count()
    
    # To save memory, collect top degrees by count and sample the rest
    top_degrees = degree_counts.orderBy(desc("count")).limit(1000)
    other_degrees = degree_counts.orderBy("count").limit(5000)
    
    # Combine and convert to pandas
    degree_pd = top_degrees.union(other_degrees).toPandas()
    
    print(f"Visualizing {len(degree_pd)} unique degree values...")
    
    # Create multiple visualizations for degree distribution
    plt.figure(figsize=(16, 10))
    
    # Plot 1: Log-Log Plot of Degree Distribution
    plt.subplot(2, 2, 1)
    sns.scatterplot(data=degree_pd, x='degree', y='count')
    plt.title('Node Degree Distribution (Log-Log Scale)')
    plt.xlabel('Degree (log scale)')
    plt.ylabel('Number of Nodes (log scale)')
    plt.xscale('log')
    plt.yscale('log')
    
    # Plot 2: Histogram of Degree Distribution (simplified)
    plt.subplot(2, 2, 2)
    plt.hist(
        degree_pd['degree'], 
        bins=min(50, len(degree_pd)), 
        weights=degree_pd['count'],
        log=True
    )
    plt.title('Degree Distribution Histogram')
    plt.xlabel('Degree (log scale)')
    plt.ylabel('Frequency (log scale)')
    plt.xscale('log')
    
    # Plot 3: Cumulative Distribution Function (memory-efficient)
    plt.subplot(2, 2, 3)
    # Sort by degree
    degree_pd = degree_pd.sort_values('degree')
    # Calculate CDF
    degree_pd['cum_pct'] = degree_pd['count'].cumsum() / degree_pd['count'].sum()
    # Plot CDF
    plt.plot(degree_pd['degree'], 1 - degree_pd['cum_pct'])
    plt.title('Complementary Cumulative Distribution Function')
    plt.xlabel('Degree (log scale)')
    plt.ylabel('P(x > X)')
    plt.xscale('log')
    plt.yscale('log')
    
    # Plot 4: Degree Distribution Overview
    plt.subplot(2, 2, 4)
    degree_pd_top = degree_pd.nlargest(20, 'count')
    sns.barplot(x='degree', y='count', data=degree_pd_top)
    plt.title('Top 20 Most Common Degree Values')
    plt.xlabel('Degree')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()
    
    print(f"\nDegree distribution visualization saved to '{output_file}'")

def run_graph_analysis_incremental(spark, file_path, sample_sizes=[0.05, 0.1, 0.25]):
    """
    Run graph analysis in stages with increasing sample sizes
    to avoid memory issues
    """
    results = {}
    
    for sample_size in sample_sizes:
        try:
            print(f"\n{'='*80}")
            print(f"STARTING ANALYSIS WITH {sample_size*100}% SAMPLE SIZE")
            print(f"{'='*80}")
            
            # Load dataset with specified sample size
            vertices, edges = load_edge_dataset(spark, file_path, sample_fraction=sample_size)
            
            # Create GraphFrame
            graph = GraphFrame(vertices, edges)
            
            # Basic analysis
            degrees = graph_basic_analysis(spark, graph)
            
            # Visualize with sample size in filename
            visualize_degree_distribution(degrees, f'degree_distribution_{int(sample_size*100)}pct.png')
            
            # PageRank analysis - only if sample is small enough
            if sample_size <= 0.1:  # Only run PageRank on smaller samples
                pagerank_results = pagerank_analysis(spark, graph)
                
                # Community detection - only on smaller samples
                community_components = community_detection_safe(spark, graph)
            else:
                print("\nSkipping PageRank and Community Detection on larger samples to preserve memory")
            
            # Clear caches to free memory
            graph.unpersist()
            vertices.unpersist()
            edges.unpersist()
            degrees.unpersist()
            
            # Force garbage collection
            spark.catalog.clearCache()
            gc.collect()
            
            print(f"\n{'='*80}")
            print(f"COMPLETED ANALYSIS WITH {sample_size*100}% SAMPLE SIZE")
            print(f"{'='*80}")
            
            # Remember successful sample size
            results[sample_size] = "Success"
            
        except Exception as e:
            print(f"\nError processing {sample_size*100}% sample: {e}")
            import traceback
            traceback.print_exc()
            results[sample_size] = f"Failed: {str(e)}"
            
            # Clear everything
            spark.catalog.clearCache()
            gc.collect()
    
    return results

def main():
    # Redirect stdout to logger
    logger = OutputLogger('graph_analysis_output_m3mac.txt')
    sys.stdout = logger
    
    try:
        # Print system information
        print(f"Starting Social Graph Analysis on MacBook Air M3 (16GB)")
        print(f"Date and Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Python Version: {sys.version}")
        
        # Initialize Spark Session with optimized settings
        print("\nInitializing Spark Session with memory-optimized settings...")
        spark = create_spark_session()
        
        # File path for your edge list (CSV or TXT)
        edge_file_path = "gplus_combined.txt"  # Replace with your actual file path
        
        # Run analysis in stages with increasing sample sizes
        sample_sizes = [0.01, 0.05, 0.1]  # Start small, increase if memory allows
        print(f"\nWill attempt analysis with sample sizes: {[f'{s*100}%' for s in sample_sizes]}")
        
        results = run_graph_analysis_incremental(spark, edge_file_path, sample_sizes)
        
        # Report results
        print("\n--- FINAL RESULTS ---")
        for sample_size, status in results.items():
            print(f"Sample Size {sample_size*100}%: {status}")
        
        # Stop Spark Session
        spark.stop()
    
    except Exception as e:
        print(f"An error occurred in main: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Close the logger
        print("\nAnalysis complete. Check output files for results.")
        logger.close()
        # Reset stdout
        sys.stdout = logger.terminal

if __name__ == "__main__":
    main()