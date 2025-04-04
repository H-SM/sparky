import os
import time
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, array, lit
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
    return (SparkSession.builder
        .appName("TwitterSocialGraphAnalysis")
        .master("local[*]")  # Added explicit local master
        .config("spark.executor.memory", "12g")
        .config("spark.driver.memory", "8g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "100")
        .getOrCreate())

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
    
    return pagerank_results

def community_detection(spark, graph):
    """
    Detect Communities using Connected Components
    """
    print("\n--- Community Detection ---")
    
    # Connected Components
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
    
    return cc

def visualize_degree_distribution(degrees):
    """
    Visualize degree distribution
    """
    degree_counts = degrees.groupBy("degree").count()
    degree_pd = degree_counts.toPandas()
    
    plt.figure(figsize=(12, 6))
    sns.histplot(data=degree_pd, x='degree', y='count', kde=True)
    plt.title('Node Degree Distribution')
    plt.xlabel('Degree')
    plt.ylabel('Number of Nodes')
    plt.xscale('log')
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig('degree_distribution.png')
    plt.close()

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
        
        # Perform Analyses
        # Pass spark to functions that need it
        degrees = graph_basic_analysis(spark, graph)
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