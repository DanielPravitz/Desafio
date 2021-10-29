import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate();

val df_transactions = spark.read.option("header",true).csv("transactions.csv")
val df_contracts = spark.read.option("header", true).csv("contracts.csv")

val df_transactions_read = df_transactions.na.replace("discount_percentage", Map("null" -> "0"))

df_transactions_read.show()
df_contracts.show()

val df_liquid_value = df_transactions_read.selectExpr("client_id", "total_amount - (total_amount * (discount_percentage / 100)) as liquid_value")
df_liquid_value.show()

df_liquid_value.join(df_contracts, df_liquid_value("client_id") === df_contracts("client_id"), "inner").filter(df_contracts("is_active") === "true").selectExpr("sum(liquid_value * (percentage / 100)) as total").show()
