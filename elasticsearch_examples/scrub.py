import json
import unittest
import sys
import math

class TestConvertNumerics(unittest.TestCase):
    # Test json
    Test1 = '{"a": 1, "b": 1.2, "c": 2}'
    Test2 = '{"a": 1, "b": 1.2, "c": {"a": 2}}'
    Test3 = '{"a": 1, "b": 1.2, "c": [{"a": 2}, {"a": 2.0}]}'


    # Expected Results
    Test1_correct = '{"a": 1.0, "b": 1.2, "c": 2.0}'
    Test2_correct = '{"a": 1.0, "b": 1.2, "c": {"a": 2.0}}'
    Test3_correct = '{"a": 1.0, "b": 1.2, "c": [{"a": 2.0}, {"a": 2.0}]}'

    def test_convert_numerics_simple(self):
        "Test conversion of simple json object"
        line = json.loads(self.Test1,object_hook=convert_numerics)
        self.assertEqual(self.Test1_correct, json.dumps(line))

    def test_convert_numerics_nested(self):
        "Test conversion of json object with nested json obect"
        line = json.loads(self.Test2,object_hook=convert_numerics)
        self.assertEqual(self.Test2_correct, json.dumps(line))

    def test_convert_numerics_list(self):
        "Test conversion of json object with json objects in a list"
        line = json.loads(self.Test3,object_hook=convert_numerics)
        self.assertEqual(self.Test3_correct, json.dumps(line))

def convert_numerics(object):
    """
    Converts any integers in a given dictionary into a float
    :param object: dictionary to modify
    :type object:dict
    """
    index=0
    for x in object:
        if type(object)==list:
            x=index
        if type(object[x])==int:
            object[x]=float(object[x])
        if (type(object[x])==int) or (type(object[x])==float):
            if math.isnan(object[x]):
                object[x]=None
        if type(object[x])==str:
            if object[x]=="?":
                object[x]=None
        if type(object[x])==list:
            object[x]=convert_numerics(object[x])
        index+=1
    return object

if __name__=="__main__":
    """
    Read lines of jsonl from stdin convert the ints to floats and
    write to stdout.
    """
    for x in sys.stdin:
        line = json.loads(x,object_hook=convert_numerics)
        print(json.dumps(line))
