# Data-Quality-Query-System
This the implementation of data quality operators using different frameworks/libraries.

# What is Data Quality Query System
Data Quality Query System is a query processing framework and language designed to facilitate the profiling and data quality assessment of large data sets, which combines traditional data management and data profiling techniques for data cleansing. This framework includes a data model that seamlessly allows users to associate quality related properties with data stored in a database or file system, by providing facilities for modeling and storing those properties.

# Frameworks/libraries used in this project
In this work, three different instances of the Data Quality Query System engine are used to assess and explore the capabilities of big data computing for data quality management.\
• Python using the Pandas library\
• PySpark\
• PySpark using the Pandas library

# Quality operators implemented in this project
In total, 5 operators were implemented:\
• Completeness (C) : this query computes a completeness score for each record regarding the fields defined by the user. In order to establish whether a value is missing, apart from blanks, the operator checks the content of the corresponding cells against special symbols used to denoted missing values (e.g., 0 for some numeric fields).\
• Timeliness (T) : this query corresponds to the plan in Figure 2. Its distinctive feature is that it inhrenetly includes an expensive join that contains timeliness metadata regarding each data record.\
• Accuracy (A) : the accuracy operator assigns a score to each row and can appear in several forms, e.g., “postcode in {‘M46’,M26’,M50’}” or “shipDate > submitDate” or “newField = Field1 + Field2 + Field3 + Field2” (where names in typewriter font denote column names). In our examples, we employed the latter form.\
• Timeliness-Completeness (T+C): this is a complex query that combines two data quality operators, namely timeliness and completeness, and includes two joins.\
• Timeliness-Accuracy (T+A): similar as above, but using accuracy instead of completeness.
