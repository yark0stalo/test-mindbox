from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

products = spark.createDataFrame([
    (1, "Продукт A"),
    (2, "Продукт B"),
    (3, "Продукт C"),
    (4, "Продукт D"),
], ["product_id", "product_name"])

categories = spark.createDataFrame([
    (1, "Категория X"),
    (2, "Категория Y"),
    (3, "Категория Z"),
], ["category_id", "category_name"])

product_category = spark.createDataFrame([
    (1, 1),
    (1, 2),
    (2, 2),
    (3, 3),
], ["product_id", "category_id"])

product_category_join = products.join(product_category, "product_id", "left")

result = product_category_join.join(categories, "category_id", "left")

result = result.select("product_name", "category_name")

result.show()
