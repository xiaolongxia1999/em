import json

#序列化方法to_json最好是一个静态方法，不依赖于具体类
class Student(object):
    def __init__(self, name, age, score):
        self.name = name
        self.age = age
        self.score = score
    def to_dict(self):
        return {'name':self.name}
def to_json(self):
    return {
            'name': self.name,
            'age': self.age,
            'score': self.score
    }

s = Student('Bob', 21, 20)

#两种序列化方式均可——用静态方法序列化，接受一个对象；调用匿名函数和实例的dict方法（一般对象的实例有个内置__dict__方法）
print(json.dumps(s, default=to_json))
print(json.dumps(s, default=lambda obj: obj.__dict__))
