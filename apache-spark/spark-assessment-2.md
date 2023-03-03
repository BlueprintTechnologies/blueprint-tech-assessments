# Apache Spark Assessment 2

This Blueprint assessment is used to evaluate your comfort with Apache Spark. To submit your answer(s), create a pull request to this repo with changes to this file that include the answers to the question(s) below.

For questions about this assessment, submit an [issue](https://github.com/BlueprintTechnologies/blueprint-tech-assessments/issues)

# Essay Question

In 200 words or **more**: What are the main streaming APIs and what is the difference between them?

[Replace with your answer here]

**Coding Example**

In this challenge, you will demonstrate using Apache Spark to validate/invalidate data according to a given set of rules describing restrictions on the contents of each column (eg. min/max length/value, format etc).

1. Using the test_rules.json in this folder as a guide, create a .tsv data file that contains three columns: name, birthdate, and age. There should be at least 20 rows in the .tsv data file. At least 25% of these records should have invalid data. The .tsv should have a header.
2. Using Apache Spark, write the valid rows in one file. A row is considered valid if all columns within it are valid according to the rules defined in the rules file. 
3. Using Apache Spark, write the invalid data in another file. For the invalid data, in a dedicated column, store information about all the fields which were invalid 
