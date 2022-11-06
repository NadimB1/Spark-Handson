from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.clean.clean_spark_job import filter_clients,joint_zip_clients,Departement_generate
from src.fr.hymaia.exo2.aggregate.aggregate_spark_job import population_counter

from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_filter_age(self):
        input = spark.createDataFrame(
            [
                Row(name='Jaake',age=24,zip=75610),
                Row(name='Logan',age=14,zip=76221)
            ]
        )

        expected = spark.createDataFrame(
            [
                Row(name='Jaake',age=24,zip=75610)
            ]
        )
        
        actual = filter_clients(input)
        self.assertCountEqual(actual.collect(),expected.collect())

#*******************************************************************
    
    def test_join(self):
        input1 = spark.createDataFrame(
            [
                Row(name='Jaake',age=24,zip=75610),
                Row(name='Logan',age=14,zip=76221),
                Row(name='Max',age=20,zip=72410)
            ]
        )
        input2= spark.createDataFrame(
            [
                Row(zip=75610,city='paris'),
                Row(zip=72410,city='lyon')
            ]
        )
        

        expected = spark.createDataFrame(
            [
                Row(name='Jaake',age=24,zip=75610,city='paris'),
                Row(name='Max',age=20,zip=72410,city='lyon')
            ]
        )
        
        
        actual = joint_zip_clients(input1,input2)
        self.assertCountEqual(actual.collect(),expected.collect())

#*******************************************************************

    def test_Dep_generating(self):
        input = spark.createDataFrame(
            [
                Row(name='Jaake',age=24,zip=75610, city='paris'),
                Row(name='Logan',age=18,zip=20190,city='lyon'),
                Row(name='Max',age=20,zip=72410,city='grenoble')
            ]
        )

        expected = spark.createDataFrame(
            [
                Row(name='Jaake',age=24,zip=75610,city='paris',departement='75'),
                Row(name='Logan',age=18,zip=20190,city='lyon',departement='2A'),
                Row(name='Max',age=20,zip=72410,city='grenoble',departement='72')
            ]
        )
        
        actual = Departement_generate(input)
        self.assertCountEqual(actual.collect(),expected.collect())

#*******************************************************************

    def test_pop_counter(self):
        input = spark.createDataFrame(
            [
                Row(name='Jaake',age=24,zip=75610,city='paris',departement='75'),
                Row(name='Logan',age=18,zip=20190,city='lyon',departement='2A'),
                Row(name='Max',age=20,zip=72410,city='grenoble',departement='72'),
                Row(name='Arnold',age=19,zip=20190,city='lyon',departement='2A'),
            ]
        )

        expected = spark.createDataFrame(
            [
                Row(departement='75',np_people=1),
                Row(departement='72',np_people=1),
                Row(departement='2A',np_people=2)
            ]
        )
        
        actual = population_counter(input)
        self.assertCountEqual(actual.collect(),expected.collect())