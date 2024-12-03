from Person import Person

class Student(Person):
  def __init__(self, fname, lname, year):
    super().__init__(fname, lname)
    self.graduationyear = year

  def __str__(self):
    # return (f"Welcome {self.firstname} {self.lastname} to the class of {str(self.graduationyear)}")
    return self.printname()