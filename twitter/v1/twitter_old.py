from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count, monotonically_increasing_id, rand
import os 

class TwitterNetworkAnalysis:
    def __init__(self, spark_session, dataset_path, output_path):
        """
        Initialize the Twitter Network Analysis class
        
        :param spark_session: Active SparkSession
        :param dataset_path: Path to the Twitter network dataset
        :param output_path: Path to save analysis results
        """
        self.spark = spark_session
        self.dataset_path = dataset_path
        self.output_path = output_path
        self.nodes_df = None
        self.edges_df = None
        self.results = []

    def load_dataset(self, sample_fraction=0.1):
        """
        Load the Twitter network dataset with sampling
        
        :param sample_fraction: Fraction of nodes to sample (default 10%)
        """
        try:
            # Read nodes dataset with more robust options
            self.nodes_df = self.spark.read.csv(
                self.dataset_path, 
                header=True, 
                inferSchema=True,
                mode='PERMISSIVE'  # Handle malformed records
            )
            
            # Rename columns for clarity
            self.nodes_df = self.nodes_df.withColumnRenamed("node_id", "id") \
                .withColumnRenamed("twitter_id", "twitter_id")
            
            # More memory-efficient sampling
            total_nodes = self.nodes_df.count()
            self.nodes_df = self.nodes_df.sample(fraction=sample_fraction, seed=42)
            
            final_node_count = self.nodes_df.count()
            
            print(f"Total Nodes: {total_nodes}")
            print(f"Sampled Nodes: {final_node_count}")
            
            self.results.extend([
                f"Total Original Nodes: {total_nodes}",
                f"Total Sampled Nodes: {final_node_count}"
            ])
            
        except Exception as e:
            print(f"Error loading dataset: {e}")
            raise

    def generate_synthetic_edges(self, max_edges=100000):
        """
        Generate synthetic edges for network analysis
        Uses more memory-efficient approach
        
        :param max_edges: Maximum number of edges to generate
        """
        try:
            # Create edges more efficiently using Spark's built-in functions
            edges_df = self.nodes_df.crossJoin(self.nodes_df.withColumnRenamed("id", "dst_id")) \
                .select(
                    col("id").alias("src"),
                    col("dst_id").alias("dst")
                ) \
                .filter(col("src") != col("dst"))  # Remove self-loops
            
            # Randomly sample edges
            self.edges_df = edges_df.sample(fraction=min(1.0, max_edges / edges_df.count()), seed=42)
            
            sampled_edges = self.edges_df.count()
            
            print("\n--- Synthetic Edges Generation ---")
            print(f"Sampled Edges: {sampled_edges}")
            
            self.results.extend([
                "--- Synthetic Edges Generation ---",
                f"Sampled Edges: {sampled_edges}"
            ])
            
        except Exception as e:
            print(f"Error generating synthetic edges: {e}")
            raise

    def network_topology_analysis(self):
        """
        Perform comprehensive network topology analysis with caching
        """
        try:
            # Cache DataFrames to improve performance
            self.nodes_df.cache()
            self.edges_df.cache()

            # 1. Degree Distribution
            degree_df = self.edges_df.groupBy("src") \
                .agg(count("dst").alias("degree"))
            
            # Join with original nodes to get more context
            full_degree_df = self.nodes_df.join(
                degree_df, 
                self.nodes_df.id == degree_df.src, 
                "left_outer"
            ).fillna(0, subset=["degree"])
            
            # Get top connected users
            top_connected_users = full_degree_df.orderBy(desc("degree")).limit(50)
            
            print("\n--- Top 50 Most Connected Users ---")
            top_connected_users.show()
            
            # Convert top connected users to a list of strings for logging
            top_users_list = [
                f"User ID: {row['id']}, Degree: {row['degree']}" 
                for row in top_connected_users.collect()
            ]
            
            self.results.extend([
                "--- Top 50 Most Connected Users ---",
                *top_users_list
            ])
            
            # 2. Network Density Calculation
            total_nodes = full_degree_df.count()
            total_possible_edges = (total_nodes * (total_nodes - 1)) // 2
            actual_edges = self.edges_df.count()
            
            network_density = actual_edges / total_possible_edges if total_possible_edges > 0 else 0
            
            print("\n--- Network Characteristics ---")
            print(f"Network Density: {network_density:.4f}")
            
            self.results.extend([
                "--- Network Characteristics ---",
                f"Network Density: {network_density:.4f}"
            ])

        except Exception as e:
            print(f"Network topology analysis error: {e}")
            raise

        finally:
            # Unpersist cached DataFrames
            self.nodes_df.unpersist()
            self.edges_df.unpersist()

    def save_results(self):
        """
        Save analysis results to a text file
        """
        try:
            with open(self.output_path, 'w') as f:
                f.write("\n".join(self.results))
            print(f"\nResults saved to {self.output_path}")
        except Exception as e:
            print(f"Error saving results: {e}")

def main():
    # Initialize Spark Session with more aggressive memory settings
    spark = SparkSession.builder \
            .appName("TwitterNetworkAnalysis") \
            .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "10g") \
        .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.default.parallelism", "200") \
    .getOrCreate()
    
    work_dir = os.path.expanduser("~/sparky")

    csv_file_path = os.path.join(work_dir, "twitter-2010-ids.csv")
    output_file_path = os.path.join(work_dir, "twitter_network_analysis_results.txt")
    
    try:
        # Initialize Analysis
        twitter_analysis = TwitterNetworkAnalysis(spark, csv_file_path, output_file_path)
        
        # Load Data with fraction parameter
        twitter_analysis.load_dataset(sample_fraction=0.1)
        
        # Generate Synthetic Edges 
        twitter_analysis.generate_synthetic_edges(max_edges=100000)
        
        # Perform Analyses
        twitter_analysis.network_topology_analysis()
        
        # Save Results
        twitter_analysis.save_results()
    
    except Exception as e:
        print(f"Analysis failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()