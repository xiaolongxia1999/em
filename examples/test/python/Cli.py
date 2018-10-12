from py4j.java_gateway import JavaGateway

gateway = JavaGateway()
random = gateway.jvm.java.util.Random()
number1 = random.nextInt(10)
number2 = random.nextInt(10)
print(number1, number2)

obj = gateway.jvm.App()
a = obj.addtion(1,2)
print(a)

