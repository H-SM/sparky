import os
import time
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, array, lit, monotonically_increasing_id
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors

# GraphFrames and GraphX require additional imports
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from graphframes import GraphFrame

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
    Create a Spark session with optimized configurations for graph processing
    """
    # Create a temporary directory for checkpointing
    checkpoint_dir = "/tmp/spark_checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    spark = (SparkSession.builder
        .appName("SocialGraphAnalysis")
        .master("local[*]")
        .config("spark.executor.memory", "24g")  # Increased from 12g
        .config("spark.driver.memory", "16g")    # Increased from 8g
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "8g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")  # Increased storage fraction
        .config("spark.sql.shuffle.partitions", "400")  # Increased partitions
        .config("spark.default.parallelism", "200")     # Increased parallelism
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # More efficient serializer
        .config("spark.kryoserializer.buffer.max", "1g")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast joins for large graphs
        .config("spark.driver.maxResultSize", "4g")      # Increase result size limit
        .getOrCreate())
    
    # Set checkpoint directory
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    return spark

def load_edge_dataset(spark, file_path, sample_fraction=0.5):
    """
    Load edge dataset and create vertex and edge DataFrames with optional sampling
    """
    print(f"Loading data from {file_path} with sampling fraction {sample_fraction}")
    
    # Define schema for edge dataset
    schema = StructType([
        StructField("src", StringType(), True),
        StructField("dst", StringType(), True)
    ])
    
    # Read edge list
    edges_df = spark.read.schema(schema).csv(file_path, sep=" ")
    
    # Sample edges to reduce size if needed
    if sample_fraction < 1.0:
        print(f"Sampling {sample_fraction * 100}% of edges to reduce graph size")
        edges_df = edges_df.sample(fraction=sample_fraction, seed=42)
    
    # Cache the edges DataFrame to improve performance
    edges_df.cache()
    
    # Create unique vertex DataFrame
    print("Creating vertices DataFrame")
    vertices_df = edges_df.select(col("src").alias("id")).union(
        edges_df.select(col("dst").alias("id"))
    ).distinct()
    
    # Add a monotonically increasing ID as a property to help with graph processing
    vertices_df = vertices_df.withColumn("index", monotonically_increasing_id())
    
    # Cache the vertices DataFrame
    vertices_df.cache()
    
    # Trigger actions to materialize the cached DataFrames
    edge_count = edges_df.count()
    vertex_count = vertices_df.count()
    
    print(f"Loaded graph with {vertex_count} vertices and {edge_count} edges")
    
    return vertices_df, edges_df

def graph_basic_analysis(spark, graph):
    """
    Perform basic graph analysis
    """
    print("\n--- Graph Basic Analysis ---")
    
    # Checkpoint the graph
    graph.vertices.checkpoint()
    graph.edges.checkpoint()
    
    # Count vertices and edges
    vertex_count = graph.vertices.count()
    edge_count = graph.edges.count()
    
    print(f"Total Nodes: {vertex_count}")
    print(f"Total Edges: {edge_count}")
    
    # Calculate graph density
    density = edge_count / (vertex_count * (vertex_count - 1))
    print(f"Graph Density: {density:.10f}")
    
    # Degree Analysis
    print("Computing node degrees...")
    degrees = graph.degrees
    degrees.cache()
    degrees.checkpoint()
    
    # Create a temp view for SQL queries
    degrees.createOrReplaceTempView("node_degrees")
    
    # Calculate basic degree statistics
    degree_stats = spark.sql("""
        SELECT 
            MIN(degree) as min_degree,
            MAX(degree) as max_degree,
            AVG(degree) as avg_degree,
            percentile_approx(degree, 0.5) as median_degree
        FROM node_degrees
    """)
    
    print("\n--- Degree Statistics ---")
    degree_stats.show()
    
    print("\n--- Top 10 Most Connected Nodes ---")
    top_connected_nodes = spark.sql("""
        SELECT id, degree 
        FROM node_degrees 
        ORDER BY degree DESC 
        LIMIT 10
    """)
    top_connected_nodes.show(truncate=False)
    
    # Calculate degree distribution
    print("Computing degree distribution...")
    degree_dist = spark.sql("""
        SELECT 
            degree, 
            COUNT(*) as node_count,
            COUNT(*) / {0} as percentage
        FROM node_degrees 
        GROUP BY degree 
        ORDER BY degree
    """.format(vertex_count))
    
    # Checkpoint and cache the degree distribution
    degree_dist.cache()
    degree_dist.checkpoint()
    
    print("\n--- Degree Distribution Sample ---")
    degree_dist.show(10)
    
    return degrees, degree_dist

def safe_pagerank_analysis(spark, graph, edge_file_path, max_iterations=[3, 5, 10]):
    """
    Perform PageRank analysis with error recovery
    """
    print("\n--- PageRank Analysis ---")
    
    pagerank_results = {}
    for iterations in max_iterations:
        try:
            print(f"Running PageRank with {iterations} iterations...")
            
            # Set a higher tolerance to converge faster
            pr = graph.pageRank(resetProbability=0.15, maxIter=iterations, tol=0.05)
            
            # Cache and checkpoint results to avoid recomputation
            pr.vertices.cache()
            pr.vertices.checkpoint()
            
            # Store results
            pagerank_results[iterations] = pr.vertices
            
            print(f"\nPageRank Results (Iterations: {iterations})")
            pr.vertices.orderBy(desc("pagerank")).limit(10).show(truncate=False)
            
        except Exception as e:
            print(f"Error in PageRank for {iterations} iterations: {e}")
            
            # Check if SparkContext is still active, if not, recreate
            try:
                if spark.sparkContext._jsc.sc().isStopped():
                    print("Spark context has stopped. Recreating...")
                    # Recreate spark session
                    spark = create_spark_session()
                    
                    # Reload data with smaller sample
                    sample_fraction = 0.3  # Use smaller sample for recovery
                    print(f"Reloading data with smaller sample fraction: {sample_fraction}")
                    vertices, edges = load_edge_dataset(spark, edge_file_path, sample_fraction)
                    graph = GraphFrame(vertices, edges)
            except Exception as inner_e:
                print(f"Failed to recreate Spark context: {inner_e}")
                # If we can't recover, just continue to next iteration
                continue
    
    return pagerank_results

def safe_community_detection(spark, graph, edge_file_path):
    """
    Detect Communities using Connected Components with error handling
    """
    print("\n--- Community Detection ---")
    
    try:
        # Checkpoint the graph before running connected components
        graph.vertices.checkpoint()
        graph.edges.checkpoint()
        
        print("Running connected components algorithm...")
        cc = graph.connectedComponents()
        
        # Cache and checkpoint results
        cc.cache()
        cc.checkpoint()
        
        # Count and analyze components
        cc.createOrReplaceTempView("communities")
        
        # Get component sizes
        print("Analyzing community sizes...")
        component_sizes = spark.sql("""
            SELECT component, COUNT(*) as component_size
            FROM communities
            GROUP BY component 
            ORDER BY component_size DESC
        """)
        
        # Cache and checkpoint component sizes
        component_sizes.cache()
        component_sizes.checkpoint()
        
        print("Top 10 Largest Community Sizes:")
        component_sizes.show(10, truncate=False)
        
        # Calculate statistics on community sizes
        print("Calculating community statistics...")
        community_stats = spark.sql("""
            SELECT 
                COUNT(*) as total_communities,
                MIN(component_size) as min_community_size,
                MAX(component_size) as max_community_size,
                AVG(component_size) as avg_community_size,
                percentile_approx(component_size, 0.5) as median_community_size
            FROM (
                SELECT component, COUNT(*) as component_size
                FROM communities
                GROUP BY component
            )
        """)
        
        print("\n--- Community Statistics ---")
        community_stats.show()
        
        return cc, component_sizes
        
    except Exception as e:
        print(f"Error in community detection: {e}")
        
        # Check if SparkContext is still active, if not, recreate
        try:
            if spark.sparkContext._jsc.sc().isStopped():
                print("Spark context has stopped. Recreating...")
                # Recreate spark session
                spark = create_spark_session()
                
                # Reload data with smaller sample
                sample_fraction = 0.2  # Use even smaller sample for recovery
                print(f"Reloading data with smaller sample fraction: {sample_fraction}")
                vertices, edges = load_edge_dataset(spark, edge_file_path, sample_fraction)
                graph = GraphFrame(vertices, edges)
                
                # Retry community detection with smaller graph
                print("Retrying community detection with smaller graph...")
                return safe_community_detection(spark, graph, edge_file_path)
        except Exception as inner_e:
            print(f"Failed to recreate Spark context: {inner_e}")
            return None, None

def visualize_degree_distribution(degrees, output_file='degree_distribution.png'):
    degree_counts = degrees.groupBy("degree").count()
    degree_pd = degree_counts.toPandas()
    
    # Create multiple visualizations for degree distribution
    plt.figure(figsize=(18, 12))
    
    # Plot 1: Log-Log Plot of Degree Distribution
    plt.subplot(2, 2, 1)
    sns.scatterplot(data=degree_pd, x='degree', y='count')
    plt.title('Node Degree Distribution (Log-Log Scale)')
    plt.xlabel('Degree (log scale)')
    plt.ylabel('Number of Nodes (log scale)')
    plt.xscale('log')
    plt.yscale('log')
    
    # Plot 2: Histogram of Degree Distribution
    plt.subplot(2, 2, 2)
    sns.histplot(data=degree_pd, x='degree', weights='count', bins=50, log_scale=(True, True))
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
    
    # Plot 4: KDE of Degree Distribution
    plt.subplot(2, 2, 4)
    # Create weighted samples for KDE
    weighted_samples = np.repeat(degree_pd['degree'].values, degree_pd['count'].values.astype(int))
    sns.kdeplot(weighted_samples, log_scale=True)
    plt.title('Kernel Density Estimation of Degree Distribution')
    plt.xlabel('Degree (log scale)')
    plt.ylabel('Density')
    
    plt.tight_layout()
    plt.savefig('degree_distribution_analysis.png')
    plt.close()
    
    print("\nDegree distribution visualization saved to 'degree_distribution_analysis.png'")

def main():
    # Define file paths
    edge_file_path = "gplus_combined.txt"  # Replace with your actual file path
    output_log_file = "graph_analysis_output.txt"
    
    # Redirect stdout to logger
    logger = OutputLogger(output_log_file)
    sys.stdout = logger
    
    # Start timer
    start_time = time.time()
    print(f"Analysis started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Track spark session for cleanup
    spark = None
    
    try:
        # Initialize Spark Session
        print("Initializing Spark session...")
        spark = create_spark_session()
        
        # Sample fraction for loading the graph (1.0 = full graph, <1.0 = sample)
        sample_fraction = 0.5  # Adjust this value based on your memory constraints
        
        # Load dataset
        print("Loading dataset...")
        vertices, edges = load_edge_dataset(spark, edge_file_path, sample_fraction)
        
        # Create GraphFrame
        print("Creating GraphFrame...")
        graph = GraphFrame(vertices, edges)
        
        # Perform Basic Analysis
        print("Performing basic graph analysis...")
        degrees, degree_dist = graph_basic_analysis(spark, graph)
        
        # PageRank Analysis with fewer iterations and error handling
        print("Performing PageRank analysis...")
        pagerank_results = safe_pagerank_analysis(spark, graph, edge_file_path, max_iterations=[3, 5, 10])
        
        # Community Detection with error handling
        print("Performing community detection...")
        community_components, component_sizes = safe_community_detection(spark, graph, edge_file_path)
        
        # Visualize Degree Distribution if we have the data
        if degree_dist is not None:
            print("Visualizing degree distribution...")
            visualize_degree_distribution(degree_dist)
        
        # Success message
        print("\n--- Analysis Complete ---")
        elapsed_time = time.time() - start_time
        print(f"Total analysis time: {elapsed_time:.2f} seconds")
        
    except Exception as e:
        print(f"An error occurred in the main analysis: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        if spark:
            try:
                print("Stopping Spark session...")
                spark.stop()
                print("Spark session stopped")
            except:
                print("Error stopping Spark session")
        
        # Close the logger
        print(f"Analysis completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.close()
        # Reset stdout
        sys.stdout = logger.terminal

if __name__ == "__main__":
    main()