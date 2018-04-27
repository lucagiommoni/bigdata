# Bigdata
01QYDOV

## Exercises:

1. WordCount problem

  * [Exercise 02](#ex2): all file in a directory

  * [Exercise 09](#ex9) : in-mapper combiner


2. PM10 pollution analysis

  * [Exercise 3](#ex3) : Use of `KeyValueTextInputFormat`

  * Exercise 5

    * [version 1](#ex5v1)

    * [verison 2](#ex5v2) : use of **combiner**

  * [Exercise 6](#ex6) : Min and Max without custom type

  * [Exercise 10](#ex10) : Custom counters


3. Total income for each month + monthly average for each year

  * [Exercise 8](#ex8) : Use of setup an clean up method


# Word Count Problem

## Exercise 2 <a name="ex2"></a>

Number of occurrences of each word appearing in the input file

### INPUT
Unstructured textual file
```
Test of the word count program
The word program is the Hadoop hello word program
Example document for hadoop word count
```

### OUTPUT
```
test    1
of      1
the     3
...
```

## Exercise 9 <a name="ex9"></a>

Number of occurrences of each word appearing in the input file

### INPUT
Unstructured textual file
```
Test of the word count program
The word program is the Hadoop hello word program
Example document for hadoop word count
```

### OUTPUT
```
test    1
of      1
the     3
...
```


# PM10 Pollution Analysis

## Exercise 3 <a name="ex3"></a>

Report, for each sensor, the number of days with PM10 above a specific threshold (hardcoded)

### INPUT
Structured textual file
```
s1,2016-01-01   20.5
25,2016-01-01   30.1
```

### OUTPUT
```
s1   2
s2   1
```

## Exercise 5 version 1 <a name="ex5v1"></a>

Report, for each sensor, the average value of PM10

### INPUT
Structured csv textual file
```
s1,2016-01-01,20.5
25,2016-01-01,30.1
...
```

### OUTPUT
```
s1   45.4
s2   34.3
```

## Exercise 5 version 2 <a name="ex5v2"></a>

Report, for each sensor, the average value of PM10
This version will use a **combiner** and a **custom type**.

### INPUT
Structured csv textual file
```
s1,2016-01-01,20.5
25,2016-01-01,30.1
...
```

### OUTPUT
```
s1   45.4
s2   34.3
```

## Exercise 6 <a name="ex6"></a>

Report, for each sensor, the maximum and the minimum value of PM10
This version will use a **combiner** but **no custom type**.

### INPUT
Structured csv textual file
```
s1,2016-01-01,20.5
25,2016-01-01,30.1
...
```

### OUTPUT
```
s1    max=60.2_min=20.5
s2    max=52.5_min=20.4
```

## Exercise 10 <a name="ex10"></a>

Total number of records.
No reducer nor combiner are used; the mapper increment the counter each time it receives an input.

### INPUT
Structured csv textual file
```
s1,2016-01-01,20.5
25,2016-01-01,30.1
...
```

### OUTPUT
```
MYCOUNTERS.TOT_RECORDS = 6
```


## Total income for each month + monthly average for each year

## Exercise 8 <a name="ex8"></a>

Total income for each month of the year and average monthly income per year

### INPUT
```
2015-11-01    1000
2016-01-01    345
...
```

### OUTPUT
```
2015-11   2305
2015-12   1250
2015      1777.5
2016      1090.00
```
