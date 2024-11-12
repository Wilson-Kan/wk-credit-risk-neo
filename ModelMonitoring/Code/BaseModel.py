class BaseMode(object):
  def __init__(self, name, id):
    self.name = name
    self.id = id

  def __str__(self):
    return f"{self.name} {self.id}"
  
  def __repr__(self):
    return self.__str__()
  
  def model_performance(self):
    pass

  def model_features(self):
    pass