from py4j.java_gateway import JavaGateway,CallbackServerParameters

# class simple_hello(object):
#     def sayHello(self, int_value=None, string_value=None):
#         print(int_value, string_value)
#         return "Said hello to %s" ,string_value
#     class Java:
#         implements = ["py4j.examples.IHello"]
#
# simple_hello = simple_hello()
# gateway = JavaGateway(callback_server_parameters=CallbackServerParameters,
#                       python_server_entry_point=simple_hello)


class SimpleHello(object):

    def sayHello(self, int_value=None, string_value=None):
        print(int_value, string_value)
        return "Said hello to {0}".format(string_value)

    class Java:
        implements = ["py4j.examples.IHello"]

# Make sure that the python code is started first.
# Then execute: java -cp py4j.jar py4j.examples.SingleThreadClientApplication
from py4j.java_gateway import JavaGateway, CallbackServerParameters
simple_hello = SimpleHello()
gateway = JavaGateway(
    callback_server_parameters=CallbackServerParameters(),
    python_server_entry_point=simple_hello)