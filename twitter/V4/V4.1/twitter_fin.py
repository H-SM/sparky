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
from pyspark.sql.types import StructType, StructField, StringType, StringType, IntegerType
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

def load_and_save_to_hdfs(spark, file_path, hdfs_path):
    """
    Load edge dataset from local filesystem and save to HDFS
    """
    print("\n--- Loading Data to HDFS ---")
    # Define schema for edge dataset
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


def load_edge_dataset(spark, file_path):
    """
    Load edge dataset and create vertex and edge DataFrames
    """
    # Define schema for edge dataset
    schema = StructType([
        StructField("src", StringType(), True),
        StructField("dst", StringType(), True)
    ])
    
    # Read edge list
    edges_df = spark.read.schema(schema).csv(file_path, sep=" ")
    
    # Create unique vertex DataFrame
    vertices_df = edges_df.select(col("src").alias("id")).union(
        edges_df.select(col("dst").alias("id"))
    ).distinct()
    
    # Add a monotonically increasing ID as a property to help with graph processing
    vertices_df = vertices_df.withColumn("index", monotonically_increasing_id())
    
    return vertices_df, edges_df

def graph_basic_analysis(spark, graph, graph_x=None, jvm=None):
    """
    Perform basic graph analysis including detailed degree distribution
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
    
    # If GraphX is available, calculate additional metrics
    if graph_x and jvm:
        try:
            print("\n--- GraphX Additional Metrics ---")
            
            # Calculate triangle count using GraphX - proper way with GraphX graph object
            triangle_count = jvm.org.apache.spark.graphx.lib.TriangleCount.run(graph_x)
            
            # Calculate clustering coefficient
            edges_count = graph_x.edges().count()
            vertices_count = graph_x.vertices().count()
            
            # Convert to Python for calculation
            triangle_count_py = triangle_count.vertices().count()
            print(f"Triangle Count: {triangle_count_py}")
            
            # Density calculation
            if vertices_count > 1:
                density = (2.0 * edges_count) / (vertices_count * (vertices_count - 1))
                print(f"Graph Density: {density}")
            
        except Exception as e:
            print(f"Error in GraphX calculations: {e}")
            import traceback
            traceback.print_exc()
    
    return degrees

def pagerank_analysis_graphx(spark, graph_x, jvm, iterations=10):
    """
    Perform PageRank analysis using native GraphX implementation
    """
    print("\n--- GraphX PageRank Analysis ---")
    
    try:
        # Run PageRank using GraphX
        pagerank_graph = jvm.org.apache.spark.graphx.lib.PageRank.run(
            graph_x, iterations, 0.15  # resetProb = 0.15 (standard value)
        )
        
        # Get the vertices with pagerank values
        pagerank_vertices = pagerank_graph.vertices()
        
        # Convert to DataFrame for easier analysis
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("rank", spark.sparkContext._jvm.org.apache.spark.sql.types.DoubleType(), True)
        ])
        
        pagerank_rows = pagerank_vertices.toJavaRDD().map(
            lambda row: Row(id=row._1, rank=row._2)
        )
        
        pagerank_df = spark.createDataFrame(pagerank_rows, schema)
        
        # Show top PageRank nodes
        print(f"\nTop 10 PageRank Nodes (Iterations: {iterations})")
        pagerank_df.orderBy(desc("rank")).limit(10).show(truncate=False)
        
        return pagerank_df
    
    except Exception as e:
        print(f"Error in GraphX PageRank: {e}")
        import traceback
        traceback.print_exc()
        return None

def pagerank_analysis(spark, graph, max_iterations=[10, 20, 50, 100]):
    """
    Perform PageRank analysis with different iterations using GraphFrames
    """
    print("\n--- GraphFrames PageRank Analysis ---")
    
    pagerank_results = {}
    for iterations in max_iterations:
        try:
            pr = graph.pageRank(resetProbability=0.15, maxIter=iterations)
            pagerank_results[iterations] = pr.vertices
            
            print(f"\nPageRank Results (Iterations: {iterations})")
            pr.vertices.orderBy(desc("pagerank")).limit(10).show(truncate=False)
        except Exception as e:
            print(f"Error in PageRank for {iterations} iterations: {e}")
    
    return pagerank_results

def community_detection(spark, graph, graph_x=None, jvm=None):
    """
    Detect Communities using Connected Components (both GraphFrames and GraphX)
    """
    print("\n--- Community Detection ---")
    
    # GraphFrames Connected Components
    print("\n--- GraphFrames Connected Components ---")
    
    # Checkpoint the graph before running connected components
    graph.vertices.checkpoint()
    graph.edges.checkpoint()
    
    # Run Connected Components
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
    component_sizes.show(10, truncate=False)
    
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
    
    # Run GraphX Connected Components if available
    if graph_x and jvm:
        try:
            print("\n--- GraphX Connected Components ---")
            
            # Run strongly connected components algorithm
            scc = jvm.org.apache.spark.graphx.lib.StronglyConnectedComponents.run(
                graph_x, 20  # Number of iterations
            )
            
            # Convert to DataFrame
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("component", StringType(), True)
            ])
            
            scc_rows = scc.vertices().toJavaRDD().map(
                lambda row: Row(id=row._1, component=row._2)
            )
            
            scc_df = spark.createDataFrame(scc_rows, schema)
            
            # Analyze SCC results
            scc_df.createOrReplaceTempView("scc_communities")
            
            scc_component_sizes = spark.sql("""
                SELECT component, COUNT(*) as component_size
                FROM scc_communities
                GROUP BY component
                ORDER BY component_size DESC
            """)
            
            print("Top 10 Largest Strongly Connected Components:")
            scc_component_sizes.show(10, truncate=False)
            
            # Run Label Propagation algorithm for community detection
            try:
                print("\n--- GraphX Label Propagation ---")
                
                # Run Label Propagation for 10 iterations
                lpa = jvm.org.apache.spark.graphx.lib.LabelPropagation.run(graph_x, 10)
                
                # Convert to DataFrame
                lpa_rows = lpa.vertices().toJavaRDD().map(
                    lambda row: Row(id=row._1, community=row._2)
                )
                
                lpa_df = spark.createDataFrame(lpa_rows, schema)
                
                # Analyze LPA results
                lpa_df.createOrReplaceTempView("lpa_communities")
                
                lpa_component_sizes = spark.sql("""
                    SELECT community as component, COUNT(*) as component_size
                    FROM lpa_communities
                    GROUP BY community
                    ORDER BY component_size DESC
                """)
                
                print("Top 10 Largest Label Propagation Communities:")
                lpa_component_sizes.show(10, truncate=False)
                
            except Exception as e:
                print(f"Error in Label Propagation: {e}")
            
            return cc, scc_df, lpa_df
        
        except Exception as e:
            print(f"Error in GraphX components: {e}")
    
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
    logger = OutputLogger('graph_analysis_output_enhanced.txt')
    sys.stdout = logger
    
    try:
        # Initialize Spark Session
        spark = create_spark_session()
        
        # File path for your edge list (CSV or TXT)
        edge_file_path = "gplus_combined.txt"  # Replace with your actual file path
        
        # HDFS path for data storage
        hdfs_path = "/user/hadoop/social_graph_data"
        
        # Try to save to HDFS (will fallback to local if unavailable)
        # hdfs_file_path = load_and_save_to_hdfs(spark, edge_file_path, hdfs_path)
        
        # Load dataset (from HDFS if available)
        vertices, edges = load_edge_dataset(spark, edge_file_path)  # Set to True if HDFS is configured
        
        # Create GraphFrame
        graph = GraphFrame(vertices, edges)
        
        graph_x = None
        jvm = spark._jvm
    
        # Perform Analyses
        degrees = graph_basic_analysis(spark, graph, graph_x, jvm)
        
        # Run PageRank using both implementations
        pagerank_results = pagerank_analysis(spark, graph)
        pagerank_graphx = pagerank_analysis_graphx(spark, graph_x, jvm)
        
        # Community detection using both implementations
        community_components = community_detection(spark, graph, graph_x, jvm)
        
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