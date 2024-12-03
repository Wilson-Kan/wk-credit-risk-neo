class Person:
  def __init__(self, fname, lname):
    self.firstname = fname
    self.lastname = lname

  def printname(self):
    return f"{self.firstname}, {self.lastname}"
  
  def __repr__(self):
    return f"Person({self.firstname}, {self.lastname})"