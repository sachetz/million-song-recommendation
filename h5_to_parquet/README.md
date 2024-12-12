# Converting H5 files to Parquet

The attached scripts were used to explore the structure of the H5 files and convert them into the Parquet file format.

Ideally, this conversion process would be executed in a Spark environment to leverage distributed processing. However, 
due to the unavailability of H5-compatible jars on the cluster and the relatively small dataset size, the conversion was
performed locally. Additionally, a PySpark script has been attached to provide insights into how this process could be 
implemented in Spark for larger datasets or in a fully configured cluster environment.