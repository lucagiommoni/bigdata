# Bigdata
01QYDOV

## Exercises:

1. WordCount problem

  * [Exercise 02](#ex2): all file in a directory


2. PM10 pollution analysis

  * [Exercise 3](#ex3) : Use of `KeyValueTextInputFormat`

  * Exercise 5

    * [version 1](#ex5v1)

    * [verison 2](#ex5v2) : use of **combiner**


# Word Count Problem

## Exercise 2 <a name="ex2"></a>

### INPUT
Unstructured textual file
```
Hello world
Hello man
```

### OUTPUT
Number of occurences of each word appearing in the input file
```
hello   2
man     1
world   1
```

# PM10 Pollution Analysis

## Exercise 3 <a name="ex3"></a>

### INPUT
Structured textual file
```
s1,2016-01-01   20.5
25,2016-01-01   30.1
```

### OUTPUT
Report, for each sensor, the number of days with PM10 above a specific threshold (hardcoded)
```
s1   2
s2   1
```

## Exercise 5 version 1 <a name="ex5v1"></a>

### INPUT
Structured csv textual file
```
s1,2016-01-01,20.5
25,2016-01-01,30.1
...
```

### OUTPUT
Report, for each sensor, the average value of PM10
```
s1   45.4
s2   34.3
```

## Exercise 5 version 2 <a name="ex5v2"></a>

This version will use a **combiner** and a **custom type**.

### INPUT
Structured csv textual file
```
s1,2016-01-01,20.5
25,2016-01-01,30.1
...
```

### OUTPUT
Report, for each sensor, the average value of PM10
```
s1   45.4
s2   34.3
```
