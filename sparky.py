from pyspark.sql import SparkSession
import os

def create_spark_session():
    """Create and return a SparkSession"""
    return SparkSession.builder \
        .appName("SparkBasics") \
        .master("local[*]") \
        .getOrCreate()

spark = create_spark_session()
sc = spark.sparkContext

def create_rdds():
    """Demonstrate different ways to create RDDs"""
    work_dir = os.path.expanduser("~/spark_projects/tests")
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)
    
    numbers = range(1, 11)
    numbers_rdd = sc.parallelize(numbers)
    print("RDD from list:", numbers_rdd.collect())

    # making a sample text file
    text_file_path = os.path.join(work_dir, "sample.txt")
    with open(text_file_path, "w") as f:
        f.write("Hello This is HSM.\nThis is a sample spark RDD over text file.\nSparky is out to go wild and zap everyone.")
    
    text_rdd = sc.textFile(text_file_path)
    print("\nRDD from text file:", text_rdd.collect())

    # making a sample csv file
    csv_file_path = os.path.join(work_dir, "sample.csv")
    with open(csv_file_path, "w") as f:
        f.write("id,name,mob\n261723,Harman,8792327121\n223173,Sid,87923228201\n261281,Saksham,8792317121")
    
    csv_rdd = sc.textFile(csv_file_path)
    print("\nRDD from CSV:", csv_rdd.collect())
    
    return numbers_rdd, text_rdd, csv_rdd

def demonstrate_transformations(numbers_rdd, text_rdd):
    """Demonstrate various RDD transformations"""
    squared_rdd = numbers_rdd.map(lambda x: x * x)
    print("Squared numbers:", squared_rdd.collect())

    even_rdd = numbers_rdd.filter(lambda x: x % 2 == 0)
    print("\nEven numbers:", even_rdd.collect())

    words_rdd = text_rdd.flatMap(lambda line: line.split())
    print("\nWords from text:", words_rdd.collect())
    
    return squared_rdd, even_rdd, words_rdd

def demonstrate_actions(numbers_rdd):
    """Demonstrate various RDD actions"""
    # collect(), count(), reduce(), take(n) actions
    print("Collected elements:", numbers_rdd.collect())
    print("\nTotal elements:", numbers_rdd.count())
    sum_result = numbers_rdd.reduce(lambda a, b: a + b)
    print("\nSum of elements:", sum_result)
    first_5 = numbers_rdd.take(5)
    print("\nFirst 5 elements:", first_5)

def main():
    try:
        print("=== Creating RDDs ===")
        numbers_rdd, text_rdd, csv_rdd = create_rdds()
        print("\n=== Demonstrating Transformations ===")
        squared_rdd, even_rdd, words_rdd = demonstrate_transformations(numbers_rdd, text_rdd)
        print("\n=== Demonstrating Actions ===")
        demonstrate_actions(numbers_rdd)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()