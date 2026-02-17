from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from collections import defaultdict
import json


# --------------------------------------------------------
# 1. Stop old StreamingContext if exists
# --------------------------------------------------------
old_ssc = StreamingContext.getActive()
if old_ssc is not None:
    old_ssc.stop(stopSparkContext=False, graceful=True)

spark = SparkSession.builder \
    .appName("SparkStreamingKafkaSink") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()


# --------------------------------------------------------
# 2. Spark Session & Streaming Context
# --------------------------------------------------------
sc = spark.sparkContext
ssc = StreamingContext(sc, 5)   # 5-second micro-batch


# --------------------------------------------------------
# 3. Metric Functions
# --------------------------------------------------------
def extract_fruit_value(batch):
    return [(fruit, float(value)) for _, fruit, value in batch]

def compute_sum_count(records):
    sum_dict = defaultdict(float)
    count_dict = defaultdict(int)
    for fruit, val in records:
        sum_dict[fruit] += val
        count_dict[fruit] += 1
    return {'sum': dict(sum_dict), 'count': dict(count_dict)}

def compute_avg(data):
    sum_dict = data['sum']
    count_dict = data['count']
    avg_dict = {}
    for fruit in sum_dict:
        if count_dict[fruit]:
            avg_dict[fruit] = round(sum_dict[fruit] / count_dict[fruit], 2)
    data['avg'] = avg_dict
    return data

def flag_high_avg(data, threshold=3.5):
    flagged = {fruit: avg for fruit, avg in data['avg'].items() if avg > threshold}
    data['flagged'] = flagged
    return data

def format_output(data):
    return {
        "📊 Avg Summary": data['avg'],
        "🚩 High Avg Fruits": data['flagged']
    }


# --------------------------------------------------------
# Preprocessing: Independent of data type
# --------------------------------------------------------
def parse_record(record):
    """
    Checks CSV (comma), TSV (tab), JSON, or generic whitespace. Returns None if invalid.
    """
    record = record.strip()   # Removes leading/trailing whitespace and newlines so parsing is predictable.
    if not record:
        return None

    # Try JSON (Converting list into tuple)
    try:
        data = json.loads(record)
        if all(k in data for k in ['timestamp', 'fruit', 'value']):
            return (data['timestamp'], data['fruit'], float(data['value']))
    except:
        pass

    # Try CSV / TSV / space-delimited
    for sep in [',', '\t', ' ']:
        parts = record.split(sep)
        if len(parts) == 3:
            ts, fruit, val = parts
            try:
                return (ts, fruit, float(val))
            except:
                return None

    return None


# --------------------------------------------------------
# Tumbling logic (global across all partitions)
# --------------------------------------------------------
def tumbling_global(rdd, batch_size=5):    # For every "5 seconds", we might get 100 of RDD's
    # ["t1,apple,4.0", "t2,orange,5.0", "t3,banana,2.5"......,"t6,apple,2.0"]
    """
    Collects records globally and groups into batches of N.
    """
    records = rdd.map(parse_record)
    .filter(lambda x: x is not None)
    .zipWithIndex()
    .map(lambda x: (x[1] // batch_size, x[0]))
    .groupByKey().map(lambda x: list(x[1]))
    
    # filter 
    # O/P: [ ("t1", "apple", 4.0), ("t2", "orange", 5.0), ("t3", "banana", 2.5), ("t4", "apple", 3.0), ("t5", "kiwi", 7.0), ("t6", "apple", 2.0) ]

    #.zipWithIndex() \                                                    # Attaches a global index to each record....
    # O/P: [ (("t1","apple",4.0), 0), (("t2","orange",5.0), 1), (("t3","banana",2.5), 2), (("t4","apple",3.0), 3), (("t5","kiwi",7.0), 4), (("t6","apple",2.0), 5), (("t7","orange",6.0), 6),  (("t8","apple",4.0), 7).......................... ]


    # .map(lambda x: (x[1] // batch_size, x[0]))                          # For batch_size=5: indices 0–4 → bucket 0, 5–9 → bucket 1
                 # For e.x. x[0] = ("t1","apple",4.0) ,  the record x[1] = 0 ..i.e (0 // 5, ("t1","apple",4.0")) = (0, ("t1","apple",4.0))
                 # For e.x. x[0] = ("t2","orange",5.0) , the record x[1] = 1 ..i.e (1 // 5, ("t1","apple",4.0")) = (0, ("t2","orang",5.0))
                 #  [  (0, ("t1","apple",4.0)),   # idx 0 → bucket 0
                 #     (0, ("t2","orange",5.0)),  # idx 1 → bucket 0
                 #     (0, ("t3","banana",2.5)),  # idx 2 → bucket 0
                 #     (0, ("t4","apple",3.0)),   # idx 3 → bucket 0
                 #     (0, ("t5","kiwi",7.0)),    # idx 4 → bucket 0
                 #     (1, ("t6","apple",2.0))    # idx 5 → bucket 1  ]

                 
    # .groupByKey() \                                # Shuffle all items with the same bucket_id end up together.
                 # [
                 #  (0, [ ("t1","apple",4.0), ("t2","orange",5.0), ("t3","banana",2.5), ("t4","apple",3.0), ("t5","kiwi",7.0) ]), 
                 #  (1, [("t6","apple",2.0)].................................
                 # ]


    # .map(lambda x: list(x[1]))                     # Drop the bucket id, just keep lists:
                 # [ [("t1","apple",4.0), ("t2","orange",5.0), ("t3","banana",2.5), ("t4","apple",3.0), ("t5","kiwi",7.0)], 
                 # [ ("t6","apple",2.0)] ...............]
    return records

## RDD[List[(timestamp, fruit, value)]]
#  e.g. [ 
#        [
#         ('2025-07-21T12:00:01', 'apple', 4.0),
#         ('2025-07-21T12:00:02', 'orange', 5.0),
#         ('2025-07-21T12:00:03', 'banana', 2.5),
#         ('2025-07-21T12:00:04', 'apple', 3.0),
#         ('2025-07-21T12:00:05', 'kiwi', 7.0)
#        ],
#        [
#         ('2025-07-21T12:00:06', 'apple', 2.0),
#         ...
#         ]
#        ]

def to_json(rdd):   # I/p: an RDD of Python dicts (one RDD per micro-batch).
                    # {"📊 Avg Summary" : {'apple':4.0, 'banana':1.5}, "🚩High Avg Fruits": {'apple':4.0}}
    
    return rdd.map(lambda record: json.dumps(record)) 
    # json.dumps(record) serializes each dict to a JSON string (what we’ll send to Kafka).
    # Output: an RDD of JSON strings.
    # RDD of [ '{ "📊 Avg Summary": {"apple":4.0,"banana":1.5}, "🚩High Avg Fruits": {"apple":4.0} }' ]


def send_to_kafka(rdd):
    if not rdd.isEmpty():
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(rdd.map(lambda x: (x,)), ["value"])    

        # rdd.map(lambda x: (x,))                --> takes each string x and wraps it in a tuple:
        # DataFrames need rows with named columns; a lone string doesn’t have a column name until wrap it.
        # [ ('{"📊 Avg Summary": {"apple": 4.0, "banana": 1.5}, "🚩 High Avg Fruits": {"apple": 4.0}}',) ]


        # ["value"]                              --> The second argument ["value"] names the column. So, Spark builds a DataFrame with one column called "value".
        # O/P : {"📊 Avg Summary": {"apple": 4.0, "banana": 1.5}, "🚩 High Avg Fruits": {"apple": 4.0}}

        df.selectExpr("CAST(value AS STRING)") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "[2001:638:904:1068:aaaa::5577]:9092") \
            .option("topic", "test-topic") \
            .save()



# --------------------------------------------------------
# 4. Define Streaming Input & Pipeline
# --------------------------------------------------------
dbutils.fs.mkdirs("dbfs:/tmp/streaming_input_31")

lines = ssc.textFileStream("dbfs:/FileStore/Yash/")
# Step2. lines is a DStream, which is a sequence of RDDs arriving in "5 seconds" everytime, one RDD per micro-batch.
# e.g. RDD [String]  
# e.g. RDD [ "2025-07-21T12:00:01,apple,4.0", "2025-07-21T12:00:02,orange,5.0", "2025-07-21T12:00:03,banana,2.5", ...................]


# --------------------------------------------------------
# Operator Chain
# --------------------------------------------------------

op1 = lines.transform(lambda rdd: tumbling_global(rdd, batch_size=5))
# I\P RDD of lines -> ["t1,apple,4.0", "t2,orange,5.0", "t3,banana,2.5", ...]
# O\P RDD of lists of 5 tuples:
# e.g [ [("t1","apple",4.0), ("t2","orange",5.0), ... 5 items], 
# [("t6","kiwi",7.0), ... 5 items], [("t11","orange",3.0), ... 5 items] ]


op2 = op1.map(extract_fruit_value)           # -> RDD [ List [(fruit, value)] ]
                                             # [ ('apple', 4.0),('banana', 2.0),....]
op2.pprint()


op3 = op2.map(compute_sum_count)             # -> RDD [Dict with sum, count]
                                             #    { 'sum': {'apple': 12.0, 'banana': 3.0},
                                             #      'count':{'apple': 3, 'banana': 2}    }
op3.pprint()

op4 = op3.map(compute_avg)                   # -> RDD [Dict with avg]
                                             #    { 'sum': {...},
                                             #      'count': {...},
                                             #      'avg': {'apple': 4.0, 'banana': 1.5} }
op4.pprint()


op5 = op4.map(lambda d: flag_high_avg(d, threshold=3.5))  # -> RDD [Dict with flagged]
                                                          #   { 'avg': {'apple': 4.0, 'banana': 1.5},  'flagged': {'apple': 4.0} }
op5.pprint()


# Debug prints in Databricks console         # -> RDD [Dict with final output]
op6 = op5.map(format_output)                              
op6.pprint()                                 # {"📊 Avg Summary": {'apple': 4.0, 'banana': 1.5}, "🚩 High Avg Fruits": {'apple': 4.0} }



# Send to Kafka
op6_json = op6.transform(to_json)            # .transform(func) is a DStream operator that,for each micro-batch, takes that batch’s underlying RDD and applies func(rdd).
op6_json.foreachRDD(send_to_kafka)          
              

# --------------------------------------------------------
# Start Streaming
# --------------------------------------------------------
ssc.start()
print(">>> Streaming—drop 5‐line CSVs into /dbfs/tmp/streaming_input/ to trigger")

ssc.awaitTerminationOrTimeout(120)                      # Run for 120 seconds

if ssc is not None:
    ssc.stop(stopSparkContext=False, graceful=True)
