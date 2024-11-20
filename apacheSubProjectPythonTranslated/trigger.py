import collections
from abc import ABCMeta
from abc import abstractmethod


class Trigger(metaclass=ABCMeta):

  def __init__(self, subTriggers=None):
    if subTriggers is None:
      self.subTriggers = collections.emptyList()
    self.subTriggers = subTriggers

  # Optimized by LLM: Removed the MoreObjects.firstNonNull call
  def subTriggers(self):
    return self.subTriggers

  # Optimized by LLM: Removed the null check for subTriggers
  def getContinuationTrigger(self):
    subTriggerContinuations = []
    for subTrigger in self.subTriggers:
      subTriggerContinuations.append(subTrigger.getContinuationTrigger())
    return self.getContinuationTrigger(subTriggerContinuations)

  @abstractmethod
  def getContinuationTrigger(self, continuationTriggers):
    pass

  @abstractmethod
  def getWatermarkThatGuaranteesFiring(self, window):
    pass

  @abstractmethod
  def mayFinish(self):
    pass

  # Optimized by LLM: Simplified null checks for subTriggers using Objects.equals
  def isCompatible(self, other):
    if not self.__class__ == other.__class__:
      return False

    if not collections.abc.equals(self.subTriggers, other.subTriggers):
      return False

    if len(self.subTriggers) != len(other.subTriggers):
      return False

    for i in range(len(self.subTriggers)):
      if not self.subTriggers[i].isCompatible(other.subTriggers[i]):
        return False

    return True

  # Optimized by LLM: Replaced null check with isEmpty check
  def __str__(self):
    simpleName = self.__class__.__name__
    if self.__class__.__qualname__:
      simpleName = self.__class__.__qualname__.split('.')[0] + "." + simpleName
    if not self.subTriggers:
      return simpleName
    else:
      return simpleName + "(" + ", ".join(map(str, self.subTriggers)) + ")"

  # Optimized by LLM: Replaced null check with direct comparison using Objects.equals
  def __eq__(self, obj):
    if self is obj:
      return True
    if not isinstance(obj, Trigger):
      return False
    that = obj
    return collections.abc.equals(self.__class__, that.__class__) and collections.abc.equals(self.subTriggers, that.subTriggers)

  # Optimized by LLM: Used Arrays.hashCode for better handling of the list's contents
  def __hash__(self):
    return hash((self.__class__, tuple(self.subTriggers)))

  def orFinally(self, until):
    return OrFinallyTrigger(self, until)

  @abstractmethod
  class OnceTrigger(Trigger):
    def __init__(self, subTriggers):
      super().__init__(subTriggers)

    def mayFinish(self):
      return True

    def getContinuationTrigger(self):
      continuation = super().getContinuationTrigger()
      if not isinstance(continuation, OnceTrigger):
        raise Exception("Continuation of a OnceTrigger must be a OnceTrigger")
      return continuation