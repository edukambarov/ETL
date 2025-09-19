/ *
chcp
65001 & & spark - shell - i \spark\d2.scala - -conf
"spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
* /
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.

{col, collect_list, concat_ws}
import org.apache.spark.sql.

{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val
t1 = System.currentTimeMillis()
if (1 == 1){
var df1 = spark.read.option("delimiter", ",")
.option("header", "true")
//.option("encoding", "windows-1251")
.csv("/spark/fifa_s2.csv")
df1=df1
.withColumn("ID", col("ID").cast("int"))
.withColumn("Age", col("Age").cast("int"))
.withColumn("Overall", col("Overall").cast("int"))
.withColumn("Value", col("Value").cast("int"))
.withColumn("Wage", col("Wage").cast("int"))
.withColumn("International Reputation", col("International Reputation").cast("int"))
.withColumn("Skill Moves", col("Skill Moves").cast("int"))
.withColumn("Joined", col("Joined").cast("int"))
.withColumn("Weight", col("Weight").cast("int"))
.withColumn("Height", col("Height").cast("float"))
.withColumn("Release Clause", col("Release Clause").cast("float"))
.withColumn("Name", lower(col("Name"))) // Перевод в нижний регистр
.withColumn("Nationality", lower(col("Nationality"))) // Перевод в нижний регистр
.withColumn("Club", lower(col("Club"))) // Перевод в нижний регистр
.withColumn("Preferred Foot", lower(col("Preferred Foot"))) // Перевод в нижний регистр
.withColumn("Position", lower(col("Position"))) // Перевод в нижний регистр

df1 = df1.drop("Potential") // Удаляем дублирующуся колонку
df1 = df1.dropDuplicates() // Убираются дубли строк
df1 = df1.withColumn("Category", lit("")) // Добавляем новую колонку "Категория футболиста"

df1.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=root")
.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl2a")
.mode("overwrite").save()

// Разбитие колонки Категория по группам до 20, от 20 до 30, от 30 до 36 и старше 36
import java.sql._;


def sqlexecute(sql: String) =


{
    var
conn: Connection = null;
var
stmt: Statement = null;
try {
Class.forName("com.mysql.cj.jdbc.Driver");
conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?user=root&password=root");
stmt = conn.createStatement();
stmt.executeUpdate(sql);
println(sql+" complete");
} catch {
case e: Exception = > println("exception caught" + e);
}
}
sqlexecute("""UPDATE spark.tasketl2a SET Category = (CASE 
								WHEN Age<='19' THEN 'ДО 20 лет' 
								WHEN Age BETWEEN '20' AND '29' THEN 'С 20 ДО 30 лет'
								WHEN Age BETWEEN '30' AND '35' THEN 'С 30 ДО 36 лет'  
								ELSE 'СТАРШРЕ 36 лет' 
							END)""")

df1.show()

// Количество
футболистов
в
каждой
категории
val
df_load = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=root")
.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl2a").load()
val
df_distinct = df_load.select("Name", "Category").distinct()
val
df_Category = df_distinct.groupBy("Category").count()
df_Category.show
}
val
s0 = (System.currentTimeMillis() - t1) / 1000
val
s = s0 % 60
val
m = (s0 / 60) % 60
val
h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)