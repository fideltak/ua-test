from airflow.models import Variable
from os.path import exists

# Airflow Variable Check
## Use same Variables as Dag Codes deliberately
test_flag = Variable.get("dag_sample_test_enabled",default_var="true")
file_path = Variable.get("file_path",default_var="/tmp/test.txt")
if test_flag.lower() in ['true', '1', 'yes', 'maybe', 'yeah', 'yup']:
    test_flag = True
else:
    test_flag = False

# Test Codes
def dag_test(task_name):
    if test_flag:
        if task_name == "task01":
            def _task01(task):
                def _wrapper(*args):
                    task(*args)
                    print("##########################TEST: {}#################################".format(task_name))
                    print("Test Case01: Check file existance: {}".format(file_path))
                    if _check_file_existance(file_path):
                        print("Test Case01: Passed")
                    else:
                        print("Failed")
                        raise ValueError ("No file: {}".format(file_path))
                    print("########################################################################")
                return _wrapper
            return _task01
        if task_name == "task02":
            def _task02(task):
                def _wrapper(*args):
                    task(*args)
                    print("##########################TEST: {}#################################".format(task_name))
                    print("Test Case01: Check file NOT existance: {}".format(file_path))
                    if not _check_file_existance(file_path):
                        print("Test Case01: Passed")
                    else:
                        print("Failed")
                        raise ValueError ("No file: {}".format(file_path))
                    print("########################################################################")
                return _wrapper
            return _task02
        else: 
            def _others(task):
                def _wrapper(*args):
                    task(*args)
                    print("##########################TEST: {}#################################".format(task_name))
                    print("Skipped...")
                    print("Not defined test: {}".format(task_name))
                    print("########################################################################")
                return _wrapper
            return _others
    else:
        def _no_test(task):
            def _wrapper(*args):
                task(*args)
            return _wrapper
        return _no_test

# Test Methods
def _check_file_existance(file_path):
    file_exists = exists(file_path)
    return file_exists