import os
import time
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import gc

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, array, lit, monotonically_increasing_id
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors

# GraphFrames and GraphX require additional imports
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType
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
    Create a Spark session with optimized configurations for large graph processing
    """
    # Create a temporary directory for checkpointing
    checkpoint_dir = "/tmp/spark_checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    spark = (SparkSession.builder
        .appName("LargeSocialGraphAnalysis")
        .master("local[*]")
        .config("spark.executor.memory", "16g")
        .config("spark.driver.memory", "12g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.sql.shuffle.partitions", "400")  # Increased for larger dataset
        .config("spark.default.parallelism", "200")    # Increased for larger dataset
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12")
        .config("spark.driver.maxResultSize", "4g")    # Increased result size limit
        .config("spark.network.timeout", "800s")       # Increased network timeout
        .config("spark.executor.heartbeatInterval", "60s")  # Increased heartbeat interval
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast joins for very large datasets
        .config("spark.sql.broadcastTimeout", "1200s")  # Broadcast timeout
        .getOrCreate())
    
    # Set checkpoint directory
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    # Import GraphX classes
    java_import(spark._jvm, 'org.apache.spark.graphx._')
    java_import(spark._jvm, 'org.apache.spark.graphx.lib._')
    
    return spark

def load_and_save_to_hdfs(spark, file_path, hdfs_path):
    """
    Load edge dataset from local filesystem and save to HDFS
    """
    print("\n--- Loading Data to HDFS ---")
    # Define schema for edge dataset - using StringType for long node IDs
    schema = StructType([
        StructField("src", StringType(), True),
        StructField("dst", StringType(), True)
    ])
    
    # Read edge list
    edges_df = spark.read.schema(schema).csv(file_path, sep=" ")
    
    # Save to HDFS
    try:
        edges_df.write.mode("overwrite").csv(hdfs_path)
        print(f"Data successfully saved to HDFS at: {hdfs_path}")
        return hdfs_path
    except Exception as e:
        print(f"Failed to save to HDFS: {e}")
        print("Proceeding with local file...")
        return file_path

def load_edge_dataset(spark, file_path, use_hdfs=False):
    """
    Load edge dataset and create vertex and edge DataFrames with optimizations for large datasets
    """
    print("\n--- Loading Edge Dataset ---")
    start_time = time.time()
    
    # Define schema for edge dataset - using StringType for long node IDs
    schema = StructType([
        StructField("src", StringType(), True),
        StructField("dst", StringType(), True)
    ])
    
    # Read edge list
    if use_hdfs:
        file_path = f"hdfs://{file_path}"
    
    print(f"Reading edge data from: {file_path}")
    edges_df = spark.read.schema(schema).csv(file_path, sep=" ")
    
    # Cache edges to speed up subsequent operations
    edges_df.cache()
    
    # Create unique vertex DataFrame using optimized approach for large datasets
    print("Creating vertex DataFrame...")
    vertices_df = (edges_df.select(col("src").alias("id"))
                  .union(edges_df.select(col("dst").alias("id")))
                  .distinct())
    
    # Cache vertices to speed up subsequent operations
    vertices_df.cache()
    
    # Count the vertices and edges (forcing action to cache)
    vertex_count = vertices_df.count()
    edge_count = edges_df.count()
    
    end_time = time.time()
    print(f"Dataset loaded with {vertex_count} vertices and {edge_count} edges")
    print(f"Loading completed in {end_time - start_time:.2f} seconds")
    
    return vertices_df, edges_df

def convert_to_graphx(spark, vertices_df, edges_df):
    """
    Convert GraphFrames components to GraphX format for advanced algorithms
    """
    print("\n--- Converting to GraphX Format ---")
    try:
        # Create a mapping from string IDs to long IDs (required for GraphX)
        # First, add a monotonically increasing ID to the vertices
        vertices_with_index = vertices_df.withColumn("index", monotonically_increasing_id())
        vertices_with_index.cache()
        
        # Create mappings
        id_to_index = {row["id"]: row["index"] for row in vertices_with_index.collect()}
        
        # Define UDF to convert string IDs to long indices
        def get_index(id_str):
            return id_to_index.get(id_str, -1)
        
        get_index_udf = udf(get_index, LongType())
        
        # Convert edges to use long indices
        edges_with_indices = edges_df.withColumn(
            "src_idx", get_index_udf(col("src"))
        ).withColumn(
            "dst_idx", get_index_udf(col("dst"))
        ).select("src_idx", "dst_idx")
        
        # Convert to RDD for GraphX
        edges_rdd = edges_with_indices.rdd.map(lambda row: (row["src_idx"], row["dst_idx"]))
        
        # Create Vertex RDD for GraphX
        vertices_rdd = vertices_with_index.rdd.map(lambda row: (row["index"], row["id"]))
        
        # Create GraphX graph
        graph_x = spark._jvm.org.apache.spark.graphx.Graph.apply(
            spark._jsc.parallelize(vertices_rdd.collect()),
            spark._jsc.parallelize(edges_rdd.collect())
        )
        
        print("Successfully converted to GraphX format")
        return graph_x
    except Exception as e:
        print(f"Error converting to GraphX: {e}")
        import traceback
        traceback.print_exc()
        return None

def graph_basic_analysis(spark, graph, graph_x=None, jvm=None):
    """
    Perform basic graph analysis including detailed degree distribution
    """
    print("\n--- Graph Basic Analysis ---")
    
    start_time = time.time()
    vertex_count = graph.vertices.count()
    edge_count = graph.edges.count()
    
    print(f"Total Nodes: {vertex_count}")
    print(f"Total Edges: {edge_count}")
    print(f"Basic counting completed in {time.time() - start_time:.2f} seconds")
    
    # Degree Analysis
    print("\n--- Computing degree distribution ---")
    start_time = time.time()
    degrees = graph.degrees
    degrees.cache()  # Cache for multiple uses
    degrees.createOrReplaceTempView("node_degrees")
    print(f"Degree computation completed in {time.time() - start_time:.2f} seconds")
    
    print("\n--- Top 10 Most Connected Nodes ---")
    start_time = time.time()
    top_connected_nodes = spark.sql("""
        SELECT id, degree 
        FROM node_degrees 
        ORDER BY degree DESC 
        LIMIT 10
    """)
    top_connected_nodes.show(truncate=False)
    print(f"Top nodes query completed in {time.time() - start_time:.2f} seconds")
    
    # Detailed degree distribution analysis
    print("\n--- Degree Distribution Analysis ---")
    start_time = time.time()
    
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
    
    # Histogram of degree distribution with more granular buckets for large datasets
    degree_buckets = spark.sql("""
        SELECT 
            CASE
                WHEN degree BETWEEN 1 AND 10 THEN '001-010'
                WHEN degree BETWEEN 11 AND 50 THEN '011-050'
                WHEN degree BETWEEN 51 AND 100 THEN '051-100'
                WHEN degree BETWEEN 101 AND 500 THEN '101-500'
                WHEN degree BETWEEN 501 AND 1000 THEN '501-1000'
                WHEN degree BETWEEN 1001 AND 5000 THEN '1001-5000'
                WHEN degree BETWEEN 5001 AND 10000 THEN '5001-10000'
                WHEN degree > 10000 THEN '10000+'
                ELSE 'Unknown'
            END as degree_range,
            COUNT(*) as node_count
        FROM node_degrees
        GROUP BY degree_range
        ORDER BY degree_range
    """)
    
    print("Degree Distribution by Range:")
    degree_buckets.show(truncate=False)
    print(f"Degree statistics completed in {time.time() - start_time:.2f} seconds")
    
    return degrees

def pagerank_analysis_optimized(spark, graph, iterations_list=[10, 20, 50, 100], checkpoint_interval=5):
    """
    Perform PageRank analysis with multiple iteration counts for comparison
    
    Parameters:
    spark: SparkSession
    graph: GraphFrame object
    iterations_list: List of iterations to run PageRank for
    checkpoint_interval: Interval for checkpointing during computation
    
    Returns:
    Dictionary mapping iteration count to PageRank results
    """
    print(f"\n--- PageRank Analysis (Multiple Iterations) ---")
    
    results = {}
    
    for iterations in iterations_list:
        print(f"\n--- Running PageRank with {iterations} iterations ---")
        start_time = time.time()
        
        try:
            # Run PageRank with specified iterations
            pr = graph.pageRank(resetProbability=0.15, maxIter=iterations)
            
            # Show top PageRank nodes
            print(f"\nTop 10 PageRank Nodes (Iterations: {iterations})")
            top_nodes = pr.vertices.orderBy(desc("pagerank")).limit(10)
            top_nodes.show(truncate=False)
            
            # Store results
            results[iterations] = pr.vertices
            
            print(f"PageRank with {iterations} iterations completed in {time.time() - start_time:.2f} seconds")
            
            # Force checkpoint after each iteration set to free up memory
            if iterations != iterations_list[-1]:  # Don't checkpoint after the last run
                print("Checkpointing and clearing cache...")
                spark.sparkContext.clearFiles()
                gc.collect()
        
        except Exception as e:
            print(f"Error in PageRank ({iterations} iterations): {e}")
            import traceback
            traceback.print_exc()
    
    # Compare results between different iteration counts
    if len(results) > 1:
        print("\n--- PageRank Convergence Analysis ---")
        print("Comparing top-ranked nodes across different iteration counts")
        
        # Get top 5 nodes from the highest iteration count
        if iterations_list[-1] in results:
            max_iter = iterations_list[-1]
            top_final = results[max_iter].orderBy(desc("pagerank")).limit(5).collect()
            
            print(f"\nTracking top 5 nodes from {max_iter} iterations across all iteration counts:")
            for node in top_final:
                node_id = node["id"]
                print(f"\nNode {node_id} PageRank values:")
                
                for iter_count in sorted(results.keys()):
                    node_rank = results[iter_count].filter(col("id") == node_id).select("pagerank").first()
                    if node_rank:
                        print(f"  Iterations {iter_count}: {node_rank['pagerank']:.8f}")
    
    return results
    
def community_detection_optimized(spark, graph, max_iterations=10):
    """
    Detect Communities using Connected Components with optimizations for large graphs
    """
    print("\n--- Community Detection (Optimized) ---")
    start_time = time.time()
    
    # Checkpoint the graph before running connected components
    graph.vertices.checkpoint()
    graph.edges.checkpoint()
    
    print("Running Connected Components algorithm...")
    # Run Connected Components (without maxIter parameter)
    try:
        cc = graph.connectedComponents()
        cc.cache()  # Cache for multiple uses
        
        # Rest of the function remains the same...
        cc.createOrReplaceTempView("communities")
        
        component_sizes = spark.sql("""
            SELECT component, COUNT(*) as component_size
            FROM communities
            GROUP BY component 
            ORDER BY component_size DESC
            LIMIT 20
        """)
        
        print("Top 20 Largest Community Sizes:")
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
            )
        """)
        community_stats.show(truncate=False)
        
        print(f"Community detection completed in {time.time() - start_time:.2f} seconds")
        return cc
    
    except Exception as e:
        print(f"Error in community detection: {e}")
        import traceback
        traceback.print_exc()
        return None

def visualize_degree_distribution_optimized(spark, degrees):
    """
    Visualize degree distribution with optimizations for large datasets
    """
    print("\n--- Visualizing Degree Distribution ---")
    start_time = time.time()
    
    # Get aggregated degree data using SQL for efficiency
    degree_counts = spark.sql("""
        SELECT degree, COUNT(*) as count
        FROM node_degrees
        GROUP BY degree
        ORDER BY degree
    """)
    
    # Collect to driver (with limit if necessary)
    if degree_counts.count() > 10000:
        print("Warning: Very large number of unique degrees. Sampling for visualization.")
        # Use quantiles to bin data for visualization
        degree_pd = spark.sql("""
            SELECT degree, COUNT(*) as count
            FROM node_degrees
            GROUP BY degree
            ORDER BY degree
        """).sample(False, 0.1).toPandas()
    else:
        degree_pd = degree_counts.toPandas()
    
    print(f"Data preparation completed in {time.time() - start_time:.2f} seconds")
    
    # Create visualizations
    plt.figure(figsize=(18, 12))
    
    # Plot 1: Log-Log Plot of Degree Distribution
    plt.subplot(2, 2, 1)
    sns.scatterplot(data=degree_pd, x='degree', y='count')
    plt.title('Node Degree Distribution (Log-Log Scale)')
    plt.xlabel('Degree (log scale)')
    plt.ylabel('Number of Nodes (log scale)')
    plt.xscale('log')
    plt.yscale('log')
    
    # Plot 2: Histogram of Degree Distribution (with binning for large datasets)
    plt.subplot(2, 2, 2)
    # Create bins on log scale
    max_degree = degree_pd['degree'].max()
    if max_degree > 1000:
        bins = np.logspace(0, np.log10(max_degree), 50)
        plt.hist(degree_pd['degree'], weights=degree_pd['count'], bins=bins,    log=True)
    else:
        plt.hist(degree_pd['degree'], weights=degree_pd['count'], bins=50, log=True)
    plt.title('Degree Distribution Histogram')
    plt.xlabel('Degree (log scale)')
    plt.ylabel('Frequency (log scale)')
    
    # Plot 3: Cumulative Distribution Function
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
    
    # Plot 4: Power law fit (if data follows power law distribution, common in social networks)
    plt.subplot(2, 2, 4)
    
    # Only use degrees > 0 for log-log plot
    plot_data = degree_pd[degree_pd['degree'] > 0]
    log_degrees = np.log10(plot_data['degree'])
    log_counts = np.log10(plot_data['count'])
    
    # Fit a line to the log-log data (power law)
    if len(log_degrees) > 1:
        try:
            # Simple linear regression on log-log data
            coef = np.polyfit(log_degrees, log_counts, 1)
            poly1d_fn = np.poly1d(coef)
            
            # Plot the data and fit
            plt.scatter(plot_data['degree'], plot_data['count'], alpha=0.5)
            plt.plot(plot_data['degree'], 10**poly1d_fn(log_degrees), 'r-')
            plt.title(f'Power Law Fit (α ≈ {-coef[0]:.2f})')
            plt.xlabel('Degree (log scale)')
            plt.ylabel('Frequency (log scale)')
            plt.xscale('log')
            plt.yscale('log')
        except Exception as e:
            print(f"Error in power law fitting: {e}")
            plt.text(0.5, 0.5, "Power law fitting failed", ha='center', va='center')
    else:
        plt.text(0.5, 0.5, "Insufficient data for power law fit", ha='center', va='center')
    
    plt.tight_layout()
    plt.savefig('degree_distribution_large_dataset.png', dpi=300)
    plt.close()
    
    print(f"Visualization completed in {time.time() - start_time:.2f} seconds")
    print("Degree distribution visualization saved to 'degree_distribution_large_dataset.png'")

def batch_process_large_graph(spark, vertices_df, edges_df, max_partition_size=5000000):
    """
    Process very large graphs in batches if needed
    """
    total_edges = edges_df.count()
    
    if total_edges > max_partition_size:
        print(f"\n--- Large graph detected ({total_edges} edges). Using batch processing approach. ---")
        
        # Create full graph for basic analysis
        full_graph = GraphFrame(vertices_df, edges_df)
        
        # Do basic analysis on full graph
        degrees = graph_basic_analysis(spark, full_graph)
        
        # Visualize degree distribution
        visualize_degree_distribution_optimized(spark, degrees)
        
        # For more complex algorithms, sample the graph
        print("\n--- Creating sample graph for advanced algorithms ---")
        
        # Sample vertices based on degree (keep higher degree vertices)
        degrees.createOrReplaceTempView("degrees_view")
        
        # Get vertices with degree above 75th percentile or with at least 10 connections
        high_degree_vertices = spark.sql("""
            SELECT id
            FROM degrees_view
            WHERE degree >= 
                (SELECT PERCENTILE(degree, 0.75) FROM degrees_view)
                OR degree >= 10
        """)
        
        high_degree_vertices.createOrReplaceTempView("high_degree_view")
        
        # Filter edges to only include those where both endpoints are in the high degree set
        filtered_edges = spark.sql("""
            SELECT e.src, e.dst
            FROM edges_view e
            JOIN high_degree_view s ON e.src = s.id
            JOIN high_degree_view d ON e.dst = d.id
        """)
        
        filtered_vertices = spark.sql("""
            SELECT id
            FROM high_degree_view
        """)
        
        # Create sample graph
        sample_graph = GraphFrame(filtered_vertices, filtered_edges)
        
        print(f"Sample graph created with {sample_graph.vertices.count()} vertices and {sample_graph.edges.count()} edges")
        
        # Run algorithms on sample graph
        pagerank_results = pagerank_analysis_optimized(spark, sample_graph, iterations_list=[10, 20, 50, 100])
        community_components = community_detection_optimized(spark, sample_graph)
        
        return {
            "full_graph": full_graph,
            "sample_graph": sample_graph,
            "degrees": degrees,
            "pagerank": pagerank_results,
            "communities": community_components
        }
    else:
        # Process entire graph normally
        graph = GraphFrame(vertices_df, edges_df)
        
        degrees = graph_basic_analysis(spark, graph)
        visualize_degree_distribution_optimized(spark, degrees)
        pagerank_results = pagerank_analysis_optimized(spark, graph, iterations_list=[10, 20, 50, 100])
        community_components = community_detection_optimized(spark, graph)
        
        return {
            "full_graph": graph,
            "sample_graph": None,
            "degrees": degrees,
            "pagerank": pagerank_results,
            "communities": community_components
        }

def main():
    # Redirect stdout to logger
    logger = OutputLogger('gplus_graph_analysis_output.txt')
    sys.stdout = logger
    
    # Record total execution time
    total_start_time = time.time()
    
    try:
        print("=== Google+ Social Network Analysis ===")
        print(f"Analysis started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Initialize Spark Session with optimized settings
        spark = create_spark_session()
        
        # File path for your edge list
        edge_file_path = "gplus_combined.txt"  # Google+ dataset
        
        # HDFS path for data storage (if using HDFS)
        hdfs_path = "/user/hadoop/gplus_graph_data"
        
        # Register a cleanup handler
        def cleanup():
            print("\n--- Cleaning up resources ---")
            # Call garbage collection to free memory
            gc.collect()
            # Stop Spark Session
            if 'spark' in locals():
                spark.stop()
            print(f"Total execution time: {(time.time() - total_start_time):.2f} seconds")
        
        # Load dataset (from local file)
        vertices, edges = load_edge_dataset(spark, edge_file_path, use_hdfs=False)
        
        # Register temporary views for SQL queries
        vertices.createOrReplaceTempView("vertices_view")
        edges.createOrReplaceTempView("edges_view")
        
        # Process based on graph size
        results = batch_process_large_graph(spark, vertices, edges)
        
        print("\n=== Analysis Summary ===")
        print(f"Total Nodes: {results['full_graph'].vertices.count()}")
        print(f"Total Edges: {results['full_graph'].edges.count()}")
        print(f"Analysis completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total execution time: {(time.time() - total_start_time):.2f} seconds")
        
        # Clean up
        cleanup()
    
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Close the logger
        logger.close()
        # Reset stdout
        sys.stdout = logger.terminal

if __name__ == "__main__":
    main()