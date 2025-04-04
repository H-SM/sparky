import os
import time
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, array, lit, avg, min, max, stddev, expr
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors

# GraphFrames and GraphX require additional imports
from pyspark.sql.types import StructType, StructField, LongType, IntegerType
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
        .appName("TwitterSocialGraphAnalysis")
        .master("local[*]")  # Added explicit local master
        .config("spark.executor.memory", "12g")
        .config("spark.driver.memory", "8g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "100")
        .getOrCreate())
    
    # Set checkpoint directory
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    return spark

def load_edge_dataset(spark, file_path):
    """
    Load edge dataset and create vertex and edge DataFrames
    """
    # Define schema for edge dataset
    schema = StructType([
        StructField("src", LongType(), True),
        StructField("dst", LongType(), True)
    ])
    
    # Read edge list
    edges_df = spark.read.schema(schema).csv(file_path, sep=" ")
    
    # Create unique vertex DataFrame
    vertices_df = edges_df.select(col("src").alias("id")).union(
        edges_df.select(col("dst").alias("id"))
    ).distinct()
    
    # Add a sequential index as a property
    vertices_df = vertices_df.withColumn("index", 
        col("id").cast(IntegerType())
    )
    
    return vertices_df, edges_df

def graph_basic_analysis(spark, graph):
    """
    Perform basic graph analysis
    """
    print("\n--- Graph Basic Analysis ---")
    print(f"Total Nodes: {graph.vertices.count()}")
    print(f"Total Edges: {graph.edges.count()}")
    
    # Degree Analysis
    degrees = graph.degrees
    degrees.createOrReplaceTempView("node_degrees")
    
    print("\n--- Top 10 Most Connected Nodes ---")
    top_connected_nodes = spark.sql("""
        SELECT id, degree 
        FROM node_degrees 
        ORDER BY degree DESC 
        LIMIT 10
    """)
    top_connected_nodes.show()
    
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

def pagerank_analysis(spark, graph, max_iterations=[10, 20, 50, 100]):
    """
    Perform PageRank analysis with different iterations
    """
    print("\n--- PageRank Analysis ---")
    
    pagerank_results = {}
    for iterations in max_iterations:
        pr = graph.pageRank(resetProbability=0.15, maxIter=iterations)
        pagerank_results[iterations] = pr.vertices
        
        print(f"\nPageRank Results (Iterations: {iterations})")
        pr.vertices.orderBy(desc("pagerank")).limit(10).show()

        # Calculate rank statistics
        pagerank_df = pr.vertices.select(col("id"), col("pagerank").alias("rank"))
        rank_stats = pagerank_df.select(
            lit("All Nodes").alias("group"),
            min("rank").alias("min_rank"),
            max("rank").alias("max_rank"), 
            avg("rank").alias("avg_rank"),
            expr("percentile(rank, 0.5)").alias("median_rank"),
            stddev("rank").alias("stddev_rank")
        )
        
        print("\nPageRank Statistics:")
        rank_stats.show(truncate=False)
        
        # Calculate rank distribution
        rank_distribution = pagerank_df.selectExpr(
            "CASE " +
            "WHEN rank <= 0.0001 THEN 'Very Low (≤0.0001)' " +
            "WHEN rank <= 0.001 THEN 'Low (0.0001-0.001)' " +
            "WHEN rank <= 0.01 THEN 'Medium (0.001-0.01)' " +
            "WHEN rank <= 0.1 THEN 'High (0.01-0.1)' " +
            "ELSE 'Very High (>0.1)' END as rank_category"
        ).groupBy("rank_category").count().orderBy("rank_category")
        
        print("\nPageRank Distribution:")
        rank_distribution.show(truncate=False)
    
    return pagerank_results

def community_detection(spark, graph):
    """
    Detect Communities using Connected Components
    """
    print("\n--- Community Detection ---")
    
    # Connected Components
    # Checkpoint the graph before running connected components
    graph.vertices.checkpoint()
    graph.edges.checkpoint()
    
    cc = graph.connectedComponents()
    
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
    """)
    
    print("Top 10 Largest Community Sizes:")
    component_sizes.show(10)
    
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

    return cc

def visualize_degree_distribution(degrees):
    """
    Visualize degree distribution
    """
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
    # Redirect stdout to logger
    logger = OutputLogger('graph_analysis_output.txt')
    sys.stdout = logger
    
    try:
        # Initialize Spark Session
        spark = create_spark_session()
        
        # File path for your edge list (CSV or TXT)
        edge_file_path = "twitter_combined.txt"  # Updated to original txt file
        
        # Load dataset
        vertices, edges = load_edge_dataset(spark, edge_file_path)
        
        # Create GraphFrame
        graph = GraphFrame(vertices, edges)
        print("\n--- GraphFrame Created ---")
        # Pass spark to functions that need it
        degrees = graph_basic_analysis(spark, graph)
        
        # Perform GraphFrames PageRank Analysis
        pagerank_results = pagerank_analysis(spark, graph)
        community_components = community_detection(spark, graph)
        
        # Visualize Degree Distribution
        visualize_degree_distribution(degrees)
        
        # Stop Spark Session
        spark.stop()
    
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