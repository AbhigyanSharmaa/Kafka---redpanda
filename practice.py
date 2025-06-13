class Animal :
    def __init__(self , breed):
        self.breed = breed

    def canWalk(self) :
        return f"{self.breed} can walk"
    
    def sound(self):
        pass
    
class Dog(Animal):
    def __init__(self , name , Petbreed) :
        super().__init__(Petbreed)
        self.breed = Petbreed
        self.dogName = name

    def sound(self ):
        return f"{self.dogName} says woof"
    

myDog = Dog("Max" , "Dog")

print(myDog.canWalk())
print(myDog.sound())








    

        


