using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.Sql;

namespace CreatingDataFrames
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().AppName("Creator").GetOrCreate();
            
            CreateUsingRange(spark);
            CreateUsingRangeInSql(spark);
            CreateUsingRangeAndDataFrameAPI(spark);
            CreateByReadingData(spark);
            CreateUsingBuiltInType(spark);

        }

        static void CreateUsingBuiltInType(SparkSession spark)
        {
            Console.WriteLine("spark.CreateUsingBuiltInType");
            var stringArray = new string[] {"a", "b", "c"};
            var dataFrame = spark.CreateDataFrame(stringArray);

            dataFrame.Show();
            /*
             *  +---+
                | _1|
                +---+
                |  a|
                |  b|
                |  c|
                +---+
             */

            var stringList = new List<string>() {"d", "e", "f"};
            dataFrame = spark.CreateDataFrame(stringList);

            dataFrame.Show();
            /*
             *  +---+
                | _1|
                +---+
                |  d|
                |  e|
                |  f|
                +---+
             */

            var doubleList = new List<double>() {0.0, 1.1, 2.2};
            dataFrame = spark.CreateDataFrame(doubleList);

            dataFrame.Show();
            /*
             *  +---+
                | _1|
                +---+
                |0.0|
                |1.1|
                |2.2|
                +---+
             */

            dataFrame = dataFrame.WithColumnRenamed("_1", "double_column");
            dataFrame.Show();
            
            /*
             *  +-------------+
                |double_column|
                +-------------+
                |          0.0|
                |          1.1|
                |          2.2|
                +-------------+
             */

            dataFrame = dataFrame.WithColumn("literal", Functions.Lit("abc"));
            dataFrame.Show();
            /*
             *  +-------------+-------+
                |double-column|literal|
                +-------------+-------+
                |          0.0|    abc|
                |          1.1|    abc|
                |          2.2|    abc|
                +-------------+-------+
             */
            Console.WriteLine("SelectExpr");
            dataFrame =
                dataFrame.SelectExpr("double_column", "literal", "'hello' as literal2", "pmod(double_column, 2)");
            dataFrame.Show();
            /*
             *  +-------------+-------+--------+--------------------------------------+
                |double_column|literal|literal2|pmod(double_column, CAST(2 AS DOUBLE))|
                +-------------+-------+--------+--------------------------------------+
                |          0.0|    abc|   hello|                                   0.0|
                |          1.1|    abc|   hello|                                   1.1|
                |          2.2|    abc|   hello|                   0.20000000000000018|
                +-------------+-------+--------+--------------------------------------+
             */

            dataFrame = dataFrame.WithColumnRenamed("pmod(double_column, CAST(2 AS DOUBLE))", "mod_column");
            dataFrame.Show();
            /*
             *  +-------------+-------+--------+-------------------+
                |double_column|literal|literal2|         mod_column|
                +-------------+-------+--------+-------------------+
                |          0.0|    abc|   hello|                0.0|
                |          1.1|    abc|   hello|                1.1|
                |          2.2|    abc|   hello|0.20000000000000018|
                +-------------+-------+--------+-------------------+
             */
        }

        static void CreateUsingRange(SparkSession spark)
        {
            Console.WriteLine("spark.Range(1000)");
            var dataFrame = spark.Range(1000);
            dataFrame.Show(5);
            /*
             *  +---+
                | id|
                +---+
                |  0|
                |  1|
                |  2|
                |  3|
                |  4|
                +---+
             * 
             */
            
            Console.WriteLine("spark.Range(1000).WithColumn");
            dataFrame = dataFrame.WithColumn("Another Column", Functions.Lit("Literal"));
            dataFrame.Show(5);
            
            /*
             *  +---+--------------+
                | id|Another Column|
                +---+--------------+
                |  0|       Literal|
                |  1|       Literal|
                |  2|       Literal|
                |  3|       Literal|
                |  4|       Literal|
                +---+--------------+
             */
            
            Console.WriteLine("spark.Range(1000).WithColumn");
            dataFrame = dataFrame.WithColumn("Mod", Functions.Pmod(Functions.Col("id"), Functions.Lit(2)));
            dataFrame.Show(5);
            
            /*
             *  +---+--------------+---+
                | id|Another Column|Mod|
                +---+--------------+---+
                |  0|       Literal|  0|
                |  1|       Literal|  1|
                |  2|       Literal|  0|
                |  3|       Literal|  1|
                |  4|       Literal|  0|
                +---+--------------+---+
             */
        }

        static void CreateUsingRangeInSql(SparkSession spark)
        {
            Console.WriteLine("Range in SQL");
            var dataFrame = spark.Sql("select id from range(1000)");
            dataFrame.Show(5);
            /*
             *  +---+
                | id|
                +---+
                |  0|
                |  1|
                |  2|
                |  3|
                |  4|
                +---+
             */

            dataFrame = spark.Sql("select id, 'Literal' as `Another Column` from range(1000)");
            dataFrame.Show(5);
            
            /*
             *  +---+--------------+
                | id|Another Column|
                +---+--------------+
                |  0|       Literal|
                |  1|       Literal|
                |  2|       Literal|
                |  3|       Literal|
                |  4|       Literal|
                +---+--------------+
             */
            
            dataFrame = spark.Sql("select id, 'Literal' as `Another Column`, pmod(id, 2) as `Mod`  from range(1000)");
            dataFrame.Show(5);
            
            /*
             *  +---+--------------+---+
                | id|Another Column|Mod|
                +---+--------------+---+
                |  0|       Literal|  0|
                |  1|       Literal|  1|
                |  2|       Literal|  0|
                |  3|       Literal|  1|
                |  4|       Literal|  0|
                +---+--------------+---+
             */
        }
        
        static void CreateUsingRangeAndDataFrameAPI(SparkSession spark)
        {
            Console.WriteLine("spark.Sql");
            var dataFrame = spark.Sql("select id from range(1000)");
            dataFrame.Show(5);
            /*
             *  +---+
                | id|
                +---+
                |  0|
                |  1|
                |  2|
                |  3|
                |  4|
                +---+
             * 
             */
            
            Console.WriteLine("spark.Sql().WithColumn");
            dataFrame = dataFrame.WithColumn("Another Column", Functions.Lit("Literal"));
            dataFrame.Show(5);
            
            /*
             *  +---+--------------+
                | id|Another Column|
                +---+--------------+
                |  0|       Literal|
                |  1|       Literal|
                |  2|       Literal|
                |  3|       Literal|
                |  4|       Literal|
                +---+--------------+
             */
            
            Console.WriteLine("spark.Sql().WithColumn");
            dataFrame = dataFrame.WithColumn("Mod", Functions.Pmod(Functions.Col("id"), Functions.Lit(2)));
            dataFrame.Show(5);
            
            /*
             *  +---+--------------+---+
                | id|Another Column|Mod|
                +---+--------------+---+
                |  0|       Literal|  0|
                |  1|       Literal|  1|
                |  2|       Literal|  0|
                |  3|       Literal|  1|
                |  4|       Literal|  0|
                +---+--------------+---+
             */
        }
        
        static void CreateByReadingData(SparkSession spark)
        {
            var tempPath = System.IO.Path.GetTempFileName();
            File.WriteAllText(tempPath, "[{\"name\": \"ed\"},{\"name\": \"edd\"},{\"name\": \"eddie\"}]");
            
            Console.WriteLine("spark.Read()");
            var dataFrame = spark.Read().Json(tempPath);
            dataFrame.Show(5);
            
            /*
             *  +-----+
                | name|
                +-----+
                |   ed|
                |  edd|
                |eddie|
                +-----+
             */
        }
    }
}