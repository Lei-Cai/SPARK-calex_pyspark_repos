# pyspark_udf

- 创建数据框

```
aa = spark.createDataFrame([('01',['001','004'],['002','003','004']),('02',['003'],['002','003','004']),('03',['005','004','006'],['002','003','004'])], ['id','list_1','list_2'])

aa.show()
```

- 1、差集

```
diffList = lambda x: [x[0][i] for i in np.where([i not in x[1] for i in x[0]])[0]]
diffList_udf = udf(diffList,ArrayType(StringType(), containsNull=False))

bb = aa.select(diffList_udf(F.array(col('list_1'),col('list_2'))).name('sss'))
bb.show()
```


- 2、计算列表长度

```
list_length_udf = udf(lambda x : len(x),IntegerType())
cc = aa.withColumn('length',list_length_udf(col('list_2')).name('length'))
cc.show()
```

- 3、两列合并，并删除相同的保持顺序

```
def uniqueList(x):
    a = list(set(x))
    a.sort(key=x.index)
    return a
uniqueList_udf = udf(lambda x:uniqueList(x[0]+x[1]),ArrayType(StringType(),containsNull=False))

dd = aa.select(uniqueList_udf(F.array(col('list_1'),col('list_2'))).name('list_3'))
dd.show()
```
