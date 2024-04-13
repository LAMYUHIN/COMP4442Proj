from flask import Flask, render_template, request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

application = Flask(__name__, static_url_path='/static')

application.config['TEMPLATES_AUTO_RELOAD'] = True

# Create spark instance, with resource config (Please adjust as you wish)
spark = SparkSession.builder.appName("Project")\
    .getOrCreate()

# define schema of the dataset
schema = StructType([
    StructField("driverID", StringType(), nullable=False),
    StructField("carPlateNumber", StringType(), nullable=False),
    StructField("Latitude", FloatType(), nullable=False),
    StructField("Longitude", FloatType(), nullable=False),
    StructField("Speed", IntegerType(), nullable=False),
    StructField("Direction", IntegerType(), nullable=False),
    StructField("siteName", StringType(), nullable=True),
    StructField("Time", TimestampType(), nullable=False),
    StructField("isRapidlySpeedup", IntegerType(), nullable=True),
    StructField("isRapidlySlowdown", IntegerType(), nullable=True),
    StructField("isNeutralSlide",IntegerType(), nullable=True),
    StructField("isNeutralSlideFinished", IntegerType(), nullable=True),
    StructField("neutralSlideTime", IntegerType(), nullable=True),
    StructField("isOverspeed", IntegerType(), nullable=True),
    StructField("isOverspeedFinished", IntegerType(), nullable=True),
    StructField("overspeedTime", IntegerType(), nullable=True),
    StructField("isFatigueDriving", IntegerType(), nullable=True),
    StructField("isHthrottleStop", IntegerType(), nullable=True),
    StructField("isOilLeak", IntegerType(), nullable=True),
])

@application.route('/')
def index():
    return render_template('index.html')

@application.route('/analysis_form')
def analysis_form():
    return render_template('analysis_form.html')


@application.route('/analysis_report', methods=['POST','GET'])
def analysis_report():

    if request.method == "POST":
        # Collect data from the post form (date, and maybe time)
        Date = request.form['date']
        sTime = request.form['sTime']
        eTime = request.form['eTime']

        print(sTime)
        print(eTime)

        if sTime == "":
            print("null")

        # Construct the path from the date
        Path = "Data/detail_record_2017_01_"+Date+"_08_00_00"

        print(Path)


        # reading data as csv format (as it actually is), with inferSchema as I believe the data types are obvious
        data = spark.read.csv(Path, schema=schema)



        # A filter on the data, remove records where it totally has no point of interest we want to accumulate and
        # analyze only removed when all of the columns are null (no count can be contributed from that record for the
        # final analysis result)
        columns = ["isRapidlySpeedup", "isRapidlySlowDown", "isOverspeed", "isFatigueDriving", "overspeedTime",
                   "neutralSlideTime", "isHthrottleStop", "isOilLeak"]
        filtered_data = data.dropna(subset=columns, how="all")

        # If both time filters are set (further filter with time)
        if sTime != "" and eTime != "":
            TDate = str(int(Date)-1).zfill(2)
            # construct the strings to compare
            TSsTime = "2017-01-"+TDate+" "+sTime
            TSeTime = "2017-01-"+TDate+" "+eTime
            sTS = spark.sql("SELECT CAST('"+TSsTime+"' AS timestamp)").collect()[0][0]
            eTS = spark.sql("SELECT CAST('" + TSeTime + "' AS timestamp)").collect()[0][0]

            print(sTS)
            print(eTS)

            filtered_data = filtered_data.filter((col("Time") >= sTS) & (col("Time") <= eTS))

        # Extract out only the data columns we would need for analysis
        selected_data = filtered_data.select("driverID", "carPlateNumber", "isRapidlySpeedup", "isRapidlySlowDown",
                                             "isOverspeed", "isFatigueDriving", "overspeedTime", "neutralSlideTime",
                                             "isHthrottleStop", "isOilLeak")

        # Product a result, with aggregation functions
        result = selected_data.groupBy("driverID", "carPlateNumber") \
            .agg(sum("isRapidlySpeedup").alias("Total_rapid_speedup_count"),
                 sum("isRapidlySlowDown").alias("Total_rapid_slowdown_count"),
                 sum("isOilLeak").alias("Total_oil_leak_count"),
                 sum("isHthrottleStop").alias("Total_H_throttle_stop_count"),
                 sum("isOverspeed").alias("Total_overspeed_count"),
                 sum("isFatigueDriving").alias("Total_fatigue_driving_count"),
                 sum("overspeedTime").alias("Total_overspeed_time"),
                 sum("neutralSlideTime").alias("Total_neutral_slide_time"))

        # Diplay in code
        # result.show()

        # collect the report, pass to template html to display
        report = result.collect()

        return render_template('analysis_report.html', report=report)


if __name__ == '__main__':
    application.run(debug=True)
