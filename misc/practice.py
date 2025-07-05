# class Animal :
#     def __init__(self , breed):
#         self.breed = breed

#     def canWalk(self) :
#         return f"{self.breed} can walk"
    
#     def sound(self):
#         pass
    
# class Dog(Animal):
#     def __init__(self , name , Petbreed) :
#         super().__init__(Petbreed)
#         self.breed = Petbreed
#         self.dogName = name

#     def sound(self ):
#         return f"{self.dogName} says woof"
    

# myDog = Dog("Max" , "Dog")

# print(myDog.canWalk())
# print(myDog.sound())


from datetime import datetime 
time = datetime.now().isoformat()

import asyncio

di = [1,2,4,4,5,5]

msg = lambda x : [y*2 for y in di]


async def func():
    print("start")
    await asyncio.sleep(5)
    print("end")

def func1():
    print("this is a func")
asyncio.run(func())
func1()

    

        


