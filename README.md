# Bigdata
01QYDOV

## Exercises:

2. [Exercise 2](#ex2): [WordCount problem]

3. [Exercise 3](#ex3) : [PM10 pollution analysis]

4. [Exercise 4](#ex4) : [PM10 pollution analysis; custom type]

5. Exercise 5

    * [version 1](#ex5v1) : [PM10 pollution analysis; average]

    * [verison 2](#ex5v2) : [PM10 pollution analysis; combiner]

6. [Exercise 6](#ex6) : [PM10 pollution analysis; min-max]

7. [Exercise 7](#ex7) : [summarization-pattern:inverted-index]

8. [Exercise 8](#ex8) : [Income; setup-cleanup]

3. [Exercise 9](#ex9) : [WordCount problem; in-mapper combiner]

10. [Exercise 10](#ex10) : [PM10 pollution analysis; total count; custom counters]

10. [Exercise 11](#ex11) : [PM10 pollution analysis; average; summarization-pattern:numerical-summarization; custom-type; in-mapper combiner]

11. [Exercise 12](#ex12) : [PM10 pollution analysis; map-only job; user provided parameter]

13. [Exercise 13](#ex13) : [income; filtering-pattern:topK; custom type]

14. [Exercise 14](#ex14) : [dictionary; filtering-pattern:distinct]

15. [Exercise 15](#ex15) : [dictionary; filtering-pattern:distinct; unique id]

17. [Exercise 17](#ex17) : [multiple input; filtering pattern:filtering]

18. [Exercise 18](#ex18) : [PM10 pollution analysis; filtering pattern:filtering]

19. [Exercise 19](#ex19) : [PM10 pollution analysis; filtering pattern:filtering]

20. [Exercise 20](#ex20) : [PM10 pollution analysis; filtering pattern:filtering; multiple outputs]

21. [Exercise 21](#ex21) : [filtering pattern:filtering; distributed cache]

22. [Exercise 22](#ex22) : [additional parameter]

23. [Exercise 23 version 2](#ex23v2) : [additional parameter]

24. [Exercise 24](#ex24) : []

25. [Exercise 25](#ex25) : [two job]

26. [Exercise 26](#ex26) : [distributed cache]

27. [Exercise 27](#ex27) : [distributed cache; category]

28. [Exercise 28](#ex28) : [multiple input; join]

29. [Exercise 29](#ex29) : [multiple input; join]

30. [Exercise 30](#ex30) : [filter]

31. [Exercise 31](#ex31) : [map]

32. Exercise 32:

  * [version 1](#ex32v1) : [map; reduce]

  * [version 2](#ex32v2) : [dataframe]

  * [version 3](#ex32v3) : [dataset]

  * [version 4](#ex32v4) : [sql]

  * [version 5](#ex32v5) : [map; topK]

33. Exercise 33:

  * [version 1](#ex33v1) : [map; topK]

  * [version 2](#ex33v2) : [dataframe]

  * [version 3](#ex33v3) : [dataset]

  * [version 4](#ex33v4) : [sql]

34. Exercise 34:

  * [version 1](#ex34v1) : [map; reduce; filter]

  * [version 2](#ex34v2) : [map; reduce; filter; cache]

  * [version 3](#ex34v3) : [dataframe]

  * [version 4](#ex34v4) : [dataset]

  * [version 5](#ex34v5) : [sql]

35. [Exercise 35](#ex35) :

36. [Exercise 36](#ex36) :

37. [Exercise 37](#ex37) :

38. [Exercise 38](#ex38) :

39. [Exercise 39](#ex39) :

40. [Exercise 40](#ex40) :

41. [Exercise 41](#ex41) :

42. [Exercise 42](#ex42) :

43. [Exercise 43](#ex43) :

44. [Exercise 44](#ex44) :

45. [Exercise 45](#ex45) :

46. [Exercise 46](#ex46) :

47. [Exercise 47](#ex47) :

48. [Exercise 48](#ex48) :

49. [Exercise 49](#ex49) :

50. [Exercise 50](#ex50) :


# Exercise 2 <a name="ex2"></a>

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


# Exercise 3 <a name="ex3"></a>

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


# Exercise 4 <a name="ex4"></a>

Report for each zone the list of dates associated with PM10 value above a specific threshold.

Suppose threshold=50

N.B.: in this case _Combiner_ is useless because no aggregation is possible after the Mappers

### INPUT
Structured textual file
```
zone1,2016-01-01    20.5
zone2,2016-01-01    30.1
zone1,2016-01-02    60.2
zone2,2016-01-02    20.4
zone1,2016-01-03    55.5
zone2,2016-01-03    52.5
```

### OUTPUT
```
zone1   [2016-01-03,2016-01-02]
zone2   [2016-01-03]
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


# Exercise 7 <a name="ex7"></a>

Report for each word **w** the list of sentenceID of sentences containing **w**

### INPUT
```
Sentence#1	Hadoop or Spark
Sentence#2	Hadoop or Spark and Java
Sentence#3	Hadoop and Big Data
```

### OUTPUT
```
hadoop    Sentence#1,Sentence#2,Sentence#3
spark     Sentence#1,Sentence#2
java      Sentence#2
big       Sentence#3
data      Sentence#3
```


# Exercise 8 <a name="ex8"></a>

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


# Exercise 9 <a name="ex9"></a>

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


# Exercise 10 <a name="ex10"></a>

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


# Exercise 11 <a name="ex11"></a>

Report for each sensor the average value of PM10

### INPUT
```
s1,2016-01-01,20.5
s1,2016-01-02,30.1
s2,2016-01-01,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5

```

### OUTPUT
```
s1    44,4
s2    35,4
```


# Exercise 12 <a name="ex12"></a>

Output are records with a PM10 value below a **user provided** _(arguments of program)_ threshold.
**No Reducer**
**No Combiner**

### INPUT
Structured csv textual file
```
s1,2016-01-01	20.5
s1,2016-01-01	30.1
s2,2016-01-01	60.2
s2,2016-01-02	20.4
s1,2016-01-03	55.5
s2,2016-01-03	52.5

Threshold   21

```

### OUTPUT
```
s1,2016-01-01	20.5
s2,2016-01-02	20.4
```


# Exercise 13 <a name="ex13"></a>

Output the couple (date, income) for which income represents the maximum value from the input set

### INPUT
```
2015-11-01	1000
2015-11-02	1305
2015-12-01	500
2015-12-02	750
2016-01-01	345
2016-01-02	1145
2016-02-03	200
2016-02-04	500
```
### OUTPUT
```
2015-11-02	1305
```


# Exercise 14 <a name="ex14"></a>

The input words are written in the output only once.
To reduce network data, the mapper will use a linked list to keep track of each word.

### INPUT
```
Toy example
file for Hadoop
Hadoop running
example
```

### OUTPUT
```
example
file
for
hadoop
running
toy
```

# Exercise 15 <a name="ex15"></a>

The input words are written in the output only once and each word is associated with a unique integer.
To reduce network data, the mapper will use a linked list to keep track of each word.

**NB:** to implement unique identifier a private global `int` variable is used inside MyReducer class; every time a key comes to the reducer, the variable is incremented.

### INPUT
```
Toy example
file for Hadoop
Hadoop running
example
```

### OUTPUT
```
example
file
for
hadoop
running
toy
```


# Exercise 17 <a name="ex17"></a>

## Select maximum temperature for each date

Output the max temp for each date, considering the data of every input file

### INPUT
```
File#1
s1,2016-01-01,14:00,20.5
s2,2016-01-01,14:00,30.2
s1,2016-01-02,14:10,11.5
s2,2016-01-02,14:10,30.2

File#2
2016-01-01,14:00,20.1,s3
2016-01-01,14:00,10.2,s4
2016-01-02,14:15,31.5,s3
2016-01-02,14:15,20.2,s4

```

### OUTPUT
```
2016-01-01    30.2
2016-01-02    31.5
```


# Exercise 18 <a name="ex18"></a>

## Filter the readings of a set of sensors based on the value of the measurement

The output is represented by lines of the input files associated with a temperature greater than 30.0

### INPUT
```
s1,2016-01-01,14:00,20.5
s2,2016-01-01,14:00,30.2
s1,2016-01-02,14:10,11.5
s2,2016-01-02,14:10,30.2

```

### OUTPUT
```
s2,2016-01-01,14:00,30.2
s2,2016-01-02,14:10,30.2
```


# Exercise 19 <a name="ex19"></a>

## Filter the readings of a set of sensors based on the value of the measurement

The output is represented by lines of the input files associated with a temperature less than 30.0

### INPUT
```
s1,2016-01-01,14:00,20.5
s2,2016-01-01,14:00,30.2
s1,2016-01-02,14:10,11.5
s2,2016-01-02,14:10,30.2

```

### OUTPUT
```
s1,2016-01-01,14:00,20.5
s1,2016-01-02,14:10,11.5
```


# Exercise 20 <a name="ex20"></a>

## Split the readings of a set of sensors based on the value of the measurement

Output files:

- hightemp-##: a subset of input lines, from the input files, associated with a temperature greater than 30.0

- normaltemp-##: a subset of input lines, from the input files, associated with a temperature less than 30.0

N.B.: The threshold is hardcoded

### INPUT
```
s1,2016-01-01,14:00,20.5
s2,2016-01-01,14:00,30.2
s1,2016-01-02,14:10,11.5
s2,2016-01-02,14:10,30.2

```

### OUTPUT
```
File#1
s2,2016-01-01,14:00,30.2
s2,2016-01-02,14:10,30.2

File#2
s1,2016-01-01,14:00,20.5
s1,2016-01-02,14:10,11.5
```


# Exercise 21 <a name="ex21"></a>

## Stopword elimination problem

Output file:
textual file containing the same sentences of the _large input file_ without the word appearing in the _stopword file_

N.B.: the order of the sentences in the output file can be different from the order of the sentences in the input file

### INPUT
```
Large File
----------
This is the first sentence and it contains some stopwords
Second sentence with a stopword here and another here
Third sentence of the stopword example


Stopword File
-------------
a
an
and
or
i
you
we
he
she
they
it
of
the
```

### OUTPUT
```
This is first sentence contains some stopwords
Second sentence with stopword here another here
Third sentence stopword example
```


# Exercise 22 <a name="ex22"></a>

## Friends of a specific user

Each line of the input file represents a couple of friends.

The username used to find friends is passed as parameter.

### INPUT
```
User1,User2
User1,User3
User1,User4
User2,User5
```

### OUTPUT
```
User1,User5
```


# Exercise 23 version 2 <a name="ex23v2"></a>

## Potential friends of a specific user

Each line of the input file represents a couple of friends.

The username used to find friends is passed as parameter.

The output is a comma-separated list of users that can be potential friend of the user passed as parameter.

User 1 is a potential friend of User2 if they have at least one friend in common.

### INPUT
```
User1,User2
User1,User3
User1,User4
User2,User3
User2,User4
User2,User5
User5,User6
```

### OUTPUT (for User2)
```
User6
```


# Exercise 24 <a name="ex24"></a>

## Compute the list of friends for each user

### INPUT
```
User1,User2
User1,User3
User1,User4
User2,User3
User2,User4
User2,User5
User5,User6
```

### OUTPUT
```
user1   user2,user3,user4
user2   user1,user3,user4,user5
user3   user1,user2
user4   user1,user2
user5   user2,user6
user6   user5
```


# Exercise 25 <a name="ex25"></a>

## Compute the list of potential friends for each user

### INPUT
```
User1,User2
User1,User3
User1,User4
User2,User3
User2,User4
User2,User5
User5,User6
```

### OUTPUT
```
user1   user5
user2   user6
user3   user5,user4
user4   user5,user3
user5   user1,user3,user4
user6   user2
```


# Exercise 26 <a name="ex26"></a>

## Word (string) to integer conversion

2 input files:

- Large textual file

- Small dictionary file

The output file is a textual file containing the content of the input large textual file, where each word is replaced with the correspondent number written into input small dictionary file.

### INPUT
```
LARGE TEXTUAL FILE
------------------
TEST CONVERTION WORD TO INTEGER
SECOND LINE TEST WORD TO INTEGER

SMALL DICTIONARY FILE
---------------------
1	CONVERTION
2	INTEGER
3	LINE
4	SECOND
5	TEST
6	TO
7	WORD
```

### OUTPUT
```
5 1 7 6 2
4 3 5 7 6 2
```


# Exercise 27 <a name="ex27"></a>

## Categorization Rules

Output: one record for each user with the following format

- the original information of the user plus the category assigned to the user

- there is at most one rule applicable to each user

- if no rule is applicable, the user is assigned to the _unknown_ category

### INPUT
```
USERS
-----
User#1,John,Smith,M,1934,New York,Bachelor
User#2,Paul,Jones,M,1956,Dallas,College
User#3,Jenny,Smith,F,1934,Philadelphia,Bachelor
User#4,Laura,White,F,1926,New York,Doctorate

CATEGORIES
----------
Gender=M and YearOfBirth=1934 -> Category#1
Gender=M and YearOfBirth=1956 -> Category#3
Gender=F and YearOfBirth=1934 -> Category#2
Gender=F and YearOfBirth=1956 -> Category#3
```

### OUTPUT
```
User#1,John,Smith,M,1934,New York,Bachelor,Category#1
User#2,Paul,Jones,M,1956,Dallas,College,Category#3
User#3,Jenny,Smith,F,1934,Philadelphia,Bachelor,Category#2
User#4,Laura,White,F,1926,New York,Doctorate,Unknown
```


# Exercise 28 <a name="ex28"></a>

## Mapping: Questions-Answers

### INPUT
```
QUESTIONS
---------
Q1,2015-01-01,What is ..?
Q2,2015-01-03,Who invented ..

ANSWERS
-------
A1,Q1,2015-01-02,It is ..
A2,Q2,2015-01-03,John Smith
A3,Q1,2015-01-05,I think it is ..
```

### OUTPUT
```
Q1,What is ..?,A1,It is ..
Q2,Who invented ..,A2,John Smith
Q1,What is ..?,A3,I think it is ..
```


# Exercise 29 <a name="ex29"></a>

## User selection

- One record for each user that likes both Commedia and Adventure movies

- Each output record contains only Gender and YearOfBirth

- Duplicate pairs must not be removed

### INPUT
```
USERS
-----
User#1,John,Smith,M,1934,New York,Bachelor
User#2,Paul,Jones,M,1956,Dallas,College
User#3,Jenny,Smith,F,1934,Philadelphia,Bachelor

LIKES
-----
User#1,Commedia
User#1,Adventure
User#1,Drama
User#2,Commedia
User#2,Crime
User#3,Commedia
User#3,Horror
User#3,Adventure
```

### OUTPUT
```
M,1934
F,1934
```


# Exercise 32 version 1 <a name="ex32v1"></a>

## Description

Extract maximum value of pollution from CSV file using map and reduce function

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-02,30.1
s1,2016-01-01,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
60.2
```


# Exercise 32 version 2 <a name="ex32v2"></a>

## Description

Extract maximum value of pollution from CSV file using **DataFrame**.
Use `max(String colName)` aggregate function.

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-02,30.1
s1,2016-01-01,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
60.2
```


# Exercise 32 version 3 <a name="ex32v3"></a>

## Description

Extract maximum value of pollution from CSV file using **DataSet**.
Use a new class _Pollution_ to encode DataFrame.

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-02,30.1
s1,2016-01-01,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
60.2
```


# Exercise 32 version 4 <a name="ex32v4"></a>

## Description

Extract maximum value of pollution from CSV file using **SQL** on **DataSet**.
Use a new class _Pollution_ to encode DataFrame.

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-02,30.1
s1,2016-01-01,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
60.2
```


# Exercise 32 version 5 <a name="ex32v5"></a>

## Description

Extract maximum value of pollution from CSV file using **TOP** on JavaRDD.

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-02,30.1
s1,2016-01-01,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
60.2
```
<<<<<<< HEAD


# Exercise 33 version 1 <a name="ex33v1"></a>

## Description

Report the top-3 maximum values of PM10
Use **map** and **topK** methods of _JavaRDD_

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
60.2
55.5
52.5
```


# Exercise 33 version 2 <a name="ex33v2"></a>

## Description

Report the top-3 maximum values of PM10
Use dataframe

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
60.2
55.5
52.5
```


# Exercise 33 version 3 <a name="ex33v3"></a>

## Description

Report the top-3 maximum values of PM10
Use dataset

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
60.2
55.5
52.5
```


# Exercise 33 version 4 <a name="ex33v4"></a>

## Description

Report the top-3 maximum values of PM10
Use sql

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
60.2
55.5
52.5
```


# Exercise 34 version 1 <a name="ex33v1"></a>

## Description

Report the line(s) associated with the maximum value of PM10
Use map, reduce and filter

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
s1,2016-01-02,60.2
s1,2016-01-03,60.2
```


# Exercise 34 version 2 <a name="ex33v2"></a>

## Description

Report the line(s) associated with the maximum value of PM10
Use cache, map, reduce and filter

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
s1,2016-01-02,60.2
s1,2016-01-03,60.2
```


# Exercise 34 version 3 <a name="ex33v3"></a>

## Description

Report the line(s) associated with the maximum value of PM10
Use dataframe

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
s1,2016-01-02,60.2
s1,2016-01-03,60.2
```


# Exercise 34 version 4 <a name="ex33v4"></a>

## Description

Report the line(s) associated with the maximum value of PM10
Use dataset

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
s1,2016-01-02,60.2
s1,2016-01-03,60.2
```


# Exercise 34 version 5 <a name="ex33v5"></a>

## Description

Report the line(s) associated with the maximum value of PM10
Use sql 

### INPUT
```
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5
```

### OUTPUT
```
s1,2016-01-02,60.2
s1,2016-01-03,60.2
```
