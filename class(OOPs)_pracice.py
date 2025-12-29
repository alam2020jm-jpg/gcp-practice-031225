from abc import ABC, abstractmethod
class product(ABC):
    def__init__(self,price):
        self.__price=price 
    @property
    def display(self):
        print("Display the price of items:{self.__price}")
    @display.setter 
    def display(self,new_price):
        if price>0:
            self.__price=new_price
        else:
            print("Negative price not possible")
    @abstractmethod
    def gets_detail(self):
        pass 
class electronics(prodyct):
    def __init__(self,price,waranty_year):
        super().__init__(price)
        self.waranty_year=waranty_year
    def gets_details(self,waranty_year):
        print(f"Warrenty of electronics items are :-{warenty_year}")
class cloth(product):
    def __(self,price,size):
        super.__(price)
    def gets_detail(self,size):
        print(f"Size of the cloth is:- {size}")
a1=electronices(400,5)
b1=cloth(500,40)
all_objects=[a1,b1]
for i in all_objects:
    i.gets_detail()




#####!!!!!!!!@@@@@@$$$$$$$%%%%%%%%%%%&&&&&&&&&&&&&&&&**********#####
from abc import ABC, abstractmethod
class vehicloe(ABC):
    @abstractmethod
    def xyz(self):
        pass 
class car:
    def xyz(self):
        print("My Car is running over speed may be liable for penalty")
class bike:
    def xyz(self):
        print("My Bike is running over speed may be liable for penalty")
        
a=car()
a.xyz()
b=bike()
b.xyz()
#**********************#%%%%%%%#&&&&&&&&&&&&&&&&&&&&&&&&@@@@@@@@@#
class payment:
    def process_payment(self):
        print("Processing generic payment")
class creditCardPayment:
    def process_payment(self):
        print("Processing credit card payment (2% service tax added)")
class up_payment:
    def process_payment(self):
        print("Processing UPI payment (0% tax)")
a1=payment()
b1=creditCardPayment()
c1=up_payment()
list_object=[a1,b1,c1]
for i in list_object:
    i.process_payment()

#**********************#%%%%%%%#&&&&&&&&&&&&&&&&&&&&&&&&@@@@@@@@@#
class smartphone:
    def __init__(self,model_name,battery_level=50):
        self.model_name=model_name
        self.__battery_level=battery_level
    @property
    def status(self):
        print("Status of battery is :{self.__battery_level}")
    @status.setter
    def use_phone(self,charge):
        if charge>0:
            self.__battery_level+=10
            print(f"The Mobile battery is chaged at percentage:-{self.__battery_level}")
    def get_battery_status(self):
        return data 
#******************************$$$$$$$$$$$$$$$%%%%%%%%%%%%%#
class book:
    def __init__(self,title,author,is_available=True):
        self.title=title
        self.author=author 
        self.is_available=is_available 
    def borrow_book(self):
        if is_available=True:
            return is_available=False
            print("Booked Borrowed")
        else:
            prunt(f"Sorry this book is already borrowed")
    def returnp(self):
        is_available=True
        return is_available

#&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&###############
class Student:
    def __init__(self,name,roll,marks):
        self.a=name
        self.b=roll
        self.c=marks
    def display_detaisl(self):
        print(f"Name of the student is:- {self.a} RollNo is :{self.b} and Secured Marks :- {self.c}")
         return self.a,self.b,self.c 
s1=Student("zaf",100,495)
s1.display_detaisl()
s2=Student("zaff",1001,500)
s2.display_detaisl()
#&&&&&&&&&&&&&&&&&&&#
class bannkaccount:
    def __init__(self,acciountHolder,balance=0):
        self.acciountHolder=acciountHolder
        self.balance=balance
    def deposite(self,amount):
        self.balance+=amount 
        print(f"Amount deposited. New balance is {self.balance}")
        return self.balance
    def withdrawl(self,amount):
        if self.balance>=amount:
            return self.balance-=amount 
        else:
            print("Insufficient funds! Your balance is only {self.balance}")
c1=bannkaccount("zaf",5000)
c1.deposite(3000)
c1.withdrawl(1000)
