from airflow.models import Variable
from os.path import exists

# Airflow Variable Check
## Use same Variables as Dag Codes deliberately
test_flag = Variable.get("dag_sample_test_enabled",
                         default_var="true")  # Test Flag
file_path = Variable.get("file_path", default_var="/tmp/test.txt")
if test_flag.lower() in ['true', '1', 'yes', 'maybe', 'yeah', 'yup']:
    test_flag = True
else:
    test_flag = False


# Test Codes
def unit_test(name):
    if test_flag:
        if name == "create_file":

            def _test(task):

                def create_file(
                        *args):  # this method name should be same as task name
                    task(*args)
                    print(
                        "########################## UNIT TEST: {} #################################"
                        .format(name))
                    print("Test Case01: Check file existance: {}".format(
                        file_path))
                    if _check_file_existance(file_path):
                        print("Test Case01: Passed")
                    else:
                        print("Failed")
                        raise ValueError("No file: {}".format(file_path))
                    print(
                        "########################################################################"
                    )

                return create_file

            return _test
        if name == "create_file_and_remove":  # this method name should be same as task name

            def _test(task):

                def create_file_and_remove(*args):
                    task(*args)
                    print(
                        "########################## UNIT TEST: {} #################################"
                        .format(name))
                    print("Test Case01: Check file NOT existance: {}".format(
                        file_path))
                    if not _check_file_existance(file_path):
                        print("Test Case01: Passed")
                    else:
                        print("Failed")
                        raise ValueError("No file: {}".format(file_path))
                    print(
                        "########################################################################"
                    )

                return create_file_and_remove

            return _test
        else:

            def _test(task):

                def others(*args):
                    task(*args)
                    print(
                        "########################## UNIT TEST: {} #################################"
                        .format(name))
                    print("Skipped...")
                    print("Not defined test: {}".format(name))
                    print(
                        "########################################################################"
                    )

                return others

            return _test
    else:

        def _no_test(task):

            def _wrapper(*args):
                print(
                    "########################## TEST Skipped: {} #################################"
                    .format(name))
                task(*args)

            return _wrapper

        return _no_test


def task_test(name):
    if test_flag:
        if name == "task01":

            def _test(task):

                def task01(
                        *args):  # this method name should be same as task name
                    task(*args)
                    print(
                        "########################## Task TEST: {} #################################"
                        .format(name))
                    print("Test Case01: Check file existance: {}".format(
                        file_path))
                    if _check_file_existance(file_path):
                        print("Test Case01: Passed")
                    else:
                        print("Failed")
                        raise ValueError("No file: {}".format(file_path))
                    print(
                        "########################################################################"
                    )

                return task01

            return _test
        if name == "task02":  # this method name should be same as task name

            def _test(task):

                def task02(*args):
                    task(*args)
                    print(
                        "########################## Task TEST: {} #################################"
                        .format(name))
                    print("Test Case01: Check file NOT existance: {}".format(
                        file_path))
                    if not _check_file_existance(file_path):
                        print("Test Case01: Passed")
                    else:
                        print("Failed")
                        raise ValueError("No file: {}".format(file_path))
                    print(
                        "########################################################################"
                    )

                return task02

            return _test
        else:

            def _test(task):

                def others(*args):
                    task(*args)
                    print(
                        "########################## Task TEST: {} #################################"
                        .format(name))
                    print("Skipped...")
                    print("Not defined test: {}".format(name))
                    print(
                        "########################################################################"
                    )

                return others

            return _test
    else:

        def _no_test(task):

            def _wrapper(*args):
                print(
                    "########################## TEST Skipped: {} #################################"
                    .format(name))
                task(*args)

            return _wrapper

        return _no_test


# Test Methods
def _check_file_existance(file_path):
    file_exists = exists(file_path)
    return file_exists
