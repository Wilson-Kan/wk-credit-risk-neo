# Databricks notebook source
from Teacher import Teacher

t = Teacher("Wilson", "Kan", "Math")
# print(t)
t.printname
t.__str__()

# COMMAND ----------

from Student import Student

s = Student("Suzy", "Wang", "2026")
s.printname
s.printname()
s.__str__()

# COMMAND ----------

from Person import Person
p = Person("Natalie", "Chin")
p.printname()
p.__repr__()

# COMMAND ----------


