import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node album
album_node1717952253276 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-1/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1717952253276")

# Script generated for node tracks
tracks_node1717952253783 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-1/staging/track.csv"], "recurse": True}, transformation_ctx="tracks_node1717952253783")

# Script generated for node artist
artist_node1717952252636 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-1/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1717952252636")

# Script generated for node Join
album_node1717952253276DF = album_node1717952253276.toDF()
artist_node1717952252636DF = artist_node1717952252636.toDF()
Join_node1717952371001 = DynamicFrame.fromDF(album_node1717952253276DF.join(artist_node1717952252636DF, (album_node1717952253276DF['artist_id'] == artist_node1717952252636DF['id']), "left"), glueContext, "Join_node1717952371001")

# Script generated for node Join
tracks_node1717952253783DF = tracks_node1717952253783.toDF()
Join_node1717952371001DF = Join_node1717952371001.toDF()
Join_node1717952424820 = DynamicFrame.fromDF(tracks_node1717952253783DF.join(Join_node1717952371001DF, (tracks_node1717952253783DF['track_id'] == Join_node1717952371001DF['id']), "left"), glueContext, "Join_node1717952424820")

# Script generated for node Drop Fields
DropFields_node1717952514544 = DropFields.apply(frame=Join_node1717952424820, paths=["id"], transformation_ctx="DropFields_node1717952514544")

# Script generated for node Destination
Destination_node1717952528452 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1717952514544, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-1/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1717952528452")

job.commit()