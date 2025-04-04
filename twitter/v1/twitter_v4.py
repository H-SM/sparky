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

import builtins

py_min = builtins.min
py_max = builtins.max

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
    
    # PERFORMANCE OPTIMIZATION NOTE:
    # The following configurations optimize Spark for graph processing:
    # - "spark.executor.memory": Allocates memory for executors
    # - "spark.driver.memory": Allocates memory for the driver process
    # - "spark.memory.fraction": Portion of heap used for execution and storage
    # - "spark.memory.storageFraction": Portion of heap used for storage
    # - "spark.sql.shuffle.partitions": Controls the number of partitions during shuffling
    # - "spark.default.parallelism": Default number of partitions for RDDs
    
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
    
    # Record start time to measure performance
    start_time = time.time()
    cc = graph.connectedComponents()
    cc_time = time.time() - start_time
    print(f"Connected Components execution time: {cc_time:.2f} seconds")
    
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

    # Extract the top largest communities for visualization
    top_communities = component_sizes.limit(5).collect()
    largest_community_ids = [row["component"] for row in top_communities]
    
    # Create DataFrames for the top communities
    top_community_members = {}
    for comm_id in largest_community_ids:
        top_community_members[comm_id] = cc.filter(col("component") == comm_id)

    return cc, top_community_members

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

def visualize_communities(spark, graph, top_community_members):
    """
    Visualize the largest communities in the graph
    """
    import builtins as py_builtins
    
    print("\n--- Community Visualization ---")
    plt.figure(figsize=(20, 15))
    for i, (comm_id, members_df) in enumerate(top_community_members.items()):
        if i >= 4:  # Limit to 4 communities for visualization
            break
        member_ids = members_df.select("id").rdd.flatMap(lambda x: x).collect()
        community_size = len(member_ids)
        print(f"Extracting subgraph for community {comm_id} with {community_size} nodes")
        sample_size = py_builtins.min(community_size, 100)  # Limit to 100 nodes for visualization
        sampled_ids = np.random.choice(member_ids, size=sample_size, replace=False)
        id_set = set(sampled_ids)
        if community_size > 1000: 
            print(f"Community {comm_id} is large, creating simple visualization")
            plt.subplot(2, 2, i+1)
            plt.title(f"Community {comm_id}: {community_size} nodes (sampled view)")
            pos = np.random.rand(py_builtins.min(50, community_size), 2)
            plt.scatter(pos[:, 0], pos[:, 1], s=30, alpha=0.7)
            for _ in range(py_builtins.min(100, community_size)):
                idx1, idx2 = np.random.choice(range(len(pos)), 2, replace=False)
                plt.plot([pos[idx1, 0], pos[idx2, 0]], [pos[idx1, 1], pos[idx2, 1]], 'k-', alpha=0.2)
                
            plt.axis('off')
        else:
            community_edges = graph.edges.filter(
                (col("src").isin(sampled_ids)) & (col("dst").isin(sampled_ids))
            ).limit(500).collect()  # Limit edges for visualization
            plt.subplot(2, 2, i+1)
            plt.title(f"Community {comm_id}: {community_size} nodes (sampled to {sample_size})")
            positions = {}
            for node_id in sampled_ids:
                positions[node_id] = np.random.rand(2)

            # Plot nodes
            for node_id, pos in positions.items():
                plt.scatter(pos[0], pos[1], s=30)
                
            # Plot edges
            for edge in community_edges:
                if edge["src"] in positions and edge["dst"] in positions:
                    plt.plot(
                        [positions[edge["src"]][0], positions[edge["dst"]][0]],
                        [positions[edge["src"]][1], positions[edge["dst"]][1]],
                        'k-', alpha=0.2
                    )
            
            plt.axis('off')
    
    plt.tight_layout()
    plt.savefig('community_visualization.png')
    plt.close()
    
    print("\nCommunity visualization saved to 'community_visualization.png'")

def compare_graphframes_performance(spark, vertices_df, edges_df):
    """
    Compare different approaches and configurations for GraphFrames
    """
    print("\n--- GraphFrames Performance Comparison ---")
    
    # Create a standard GraphFrame (baseline)
    start_time = time.time()
    standard_graph = GraphFrame(vertices_df, edges_df)
    standard_create_time = time.time() - start_time
    print(f"Standard GraphFrame creation time: {standard_create_time:.2f} seconds")
    
    # Test 1: Standard PageRank performance
    start_time = time.time()
    pr = standard_graph.pageRank(resetProbability=0.15, maxIter=10)
    standard_pagerank_time = time.time() - start_time
    print(f"Standard PageRank (10 iterations) execution time: {standard_pagerank_time:.2f} seconds")
    
    # Test 2: Connected Components performance
    start_time = time.time()
    cc = standard_graph.connectedComponents()
    standard_cc_time = time.time() - start_time
    print(f"Standard Connected Components execution time: {standard_cc_time:.2f} seconds")
    
    # Test 3: Different partition strategy - cache the vertices and edges
    start_time = time.time()
    vertices_df_cached = vertices_df.cache()
    edges_df_cached = edges_df.cache()
    cached_graph = GraphFrame(vertices_df_cached, edges_df_cached)
    cached_create_time = time.time() - start_time
    print(f"Cached GraphFrame creation time: {cached_create_time:.2f} seconds")
    
    # Test 4: PageRank with cached graph
    start_time = time.time()
    pr_cached = cached_graph.pageRank(resetProbability=0.15, maxIter=10)
    cached_pagerank_time = time.time() - start_time
    print(f"Cached PageRank (10 iterations) execution time: {cached_pagerank_time:.2f} seconds")
    
    # Test 5: Connected Components with cached graph
    start_time = time.time()
    cc_cached = cached_graph.connectedComponents()
    cached_cc_time = time.time() - start_time
    print(f"Cached Connected Components execution time: {cached_cc_time:.2f} seconds")
    
    # Test 6: Repartition strategy
    start_time = time.time()
    vertices_repartitioned = vertices_df.repartition(20)
    edges_repartitioned = edges_df.repartition(20)
    repartitioned_graph = GraphFrame(vertices_repartitioned, edges_repartitioned)
    repartitioned_create_time = time.time() - start_time
    print(f"Repartitioned GraphFrame creation time: {repartitioned_create_time:.2f} seconds")
    
    # Test 7: PageRank with repartitioned graph
    start_time = time.time()
    pr_repartitioned = repartitioned_graph.pageRank(resetProbability=0.15, maxIter=10)
    repartitioned_pagerank_time = time.time() - start_time
    print(f"Repartitioned PageRank (10 iterations) execution time: {repartitioned_pagerank_time:.2f} seconds")
    
    # Test 8: Connected Components with repartitioned graph
    start_time = time.time()
    cc_repartitioned = repartitioned_graph.connectedComponents()
    repartitioned_cc_time = time.time() - start_time
    print(f"Repartitioned Connected Components execution time: {repartitioned_cc_time:.2f} seconds")
    
    print("\n--- Performance Comparison Summary ---")
    print("Configuration | Creation Time | PageRank Time | Connected Components Time")
    print("-" * 75)
    print(f"Standard      | {standard_create_time:.2f}s | {standard_pagerank_time:.2f}s | {standard_cc_time:.2f}s")
    print(f"Cached        | {cached_create_time:.2f}s | {cached_pagerank_time:.2f}s | {cached_cc_time:.2f}s")
    print(f"Repartitioned | {repartitioned_create_time:.2f}s | {repartitioned_pagerank_time:.2f}s | {repartitioned_cc_time:.2f}s")
    
    plt.figure(figsize=(15, 10))
    
    configs = ['Standard', 'Cached', 'Repartitioned']
    creation_times = [standard_create_time, cached_create_time, repartitioned_create_time]
    pagerank_times = [standard_pagerank_time, cached_pagerank_time, repartitioned_pagerank_time]
    cc_times = [standard_cc_time, cached_cc_time, repartitioned_cc_time]
    
    # Plotting
    bar_width = 0.25
    index = np.arange(len(configs))
    
    plt.bar(index, creation_times, bar_width, label='Creation Time')
    plt.bar(index + bar_width, pagerank_times, bar_width, label='PageRank Time')
    plt.bar(index + 2*bar_width, cc_times, bar_width, label='Connected Components Time')
    
    plt.xlabel('GraphFrames Configuration')
    plt.ylabel('Time (seconds)')
    plt.title('Performance Comparison of Different GraphFrames Configurations')
    plt.xticks(index + bar_width, configs)
    plt.legend()
    
    plt.tight_layout()
    plt.savefig('performance_comparison.png')
    plt.close()
    
    print("\nPerformance comparison visualization saved to 'performance_comparison.png'")
    
    return {
        'standard': {
            'creation': standard_create_time,
            'pagerank': standard_pagerank_time,
            'cc': standard_cc_time
        },
        'cached': {
            'creation': cached_create_time,
            'pagerank': cached_pagerank_time,
            'cc': cached_cc_time
        },
        'repartitioned': {
            'creation': repartitioned_create_time,
            'pagerank': repartitioned_pagerank_time,
            'cc': repartitioned_cc_time
        }
    }

def main():
    logger = OutputLogger('graph_analysis_output.txt')
    sys.stdout = logger
    
    try:
        spark = create_spark_session()
        
        edge_file_path = "twitter_combined.txt" 
        
        # Load dataset
        print("\n--- Loading Dataset ---")
        start_time = time.time()
        vertices, edges = load_edge_dataset(spark, edge_file_path)
        load_time = time.time() - start_time
        print(f"Dataset loading time: {load_time:.2f} seconds")
        
        # Create GraphFrame
        print("\n--- GraphFrame Created ---")
        start_time = time.time()
        graph = GraphFrame(vertices, edges)
        graph_creation_time = time.time() - start_time
        print(f"GraphFrame creation time: {graph_creation_time:.2f} seconds")
        
        degrees = graph_basic_analysis(spark, graph)
        
        pagerank_results = pagerank_analysis(spark, graph)
        
        community_components, top_communities = community_detection(spark, graph)
        
        visualize_degree_distribution(degrees)
        
        visualize_communities(spark, graph, top_communities)
        
        performance_metrics = compare_graphframes_performance(spark, vertices, edges)
        
        spark.stop()
    
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        logger.close()
        sys.stdout = logger.terminal

if __name__ == "__main__":
    main()
