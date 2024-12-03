from Person import Person

class Teacher (Person):
  def __init__(self, fname, lname, subject):
    super().__init__(fname, lname)
    self.subject = subject

  def __str__(self):
    return self.printname()