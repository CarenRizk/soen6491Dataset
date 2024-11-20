from apache_beam import PTransform
from apache_beam import PTransform
from apache_beam import WindowingStrategy
from apache_beam.coders import IterableLikeCoder
from apache_beam.transforms import FlatMapElements
from apache_beam.transforms import SimpleFunction


class Flatten:

    @staticmethod
    def pCollections():
        return Flatten.PCollections()

    @staticmethod
    def iterables():
        return Flatten.Iterables()

    class PCollections(PTransform):
        def __init__(self):
            super().__init__()

        def validateWindowCompatibility(self, windowingStrategy, input):
            # Optimized by LLM: Extracted compatibility check into a separate method
            other = input.getWindowingStrategy()
            if not windowingStrategy.getWindowFn().isCompatible(other.getWindowFn()):
                raise Exception(
                    "Inputs to Flatten had incompatible window windowFns: "
                    + str(windowingStrategy.getWindowFn())
                    + ", "
                    + str(other.getWindowFn()))

            if not windowingStrategy.getTrigger().isCompatible(other.getTrigger()):
                raise Exception(
                    "Inputs to Flatten had incompatible triggers: "
                    + str(windowingStrategy.getTrigger())
                    + ", "
                    + str(other.getTrigger()))

        def expand(self, inputs):
            windowingStrategy = None
            isBounded = PCollection.IsBounded.UNBOUNDED  # Optimized by LLM: Initialized to UNBOUNDED
            if inputs.getAll():
                windowingStrategy = inputs.get(0).getWindowingStrategy()
                for input in inputs.getAll():
                    self.validateWindowCompatibility(windowingStrategy, input)  # Optimized by LLM: Using extracted method
                    isBounded = isBounded.and(input.isBounded())
            else:
                windowingStrategy = WindowingStrategy.globalDefault()

            return PCollection.createPrimitiveOutputInternal(
                inputs.getPipeline(),
                windowingStrategy,
                isBounded,
                inputs.getAll() if inputs.getAll() else None)

    class Iterables(PTransform):
        def __init__(self):
            super().__init__()

        def isIterableLikeCoder(self, inCoder):
            # Optimized by LLM: Extracted coder check into a separate method
            return isinstance(inCoder, IterableLikeCoder)

        def expand(self, in):
            inCoder = in.getCoder()
            if not self.isIterableLikeCoder(inCoder):  # Optimized by LLM: Using extracted method
                raise Exception(
                    "expecting the input Coder<Iterable> to be an IterableLikeCoder")
            elemCoder = inCoder.getElemCoder()  # type: ignore

            return in.apply(
                "FlattenIterables",
                FlatMapElements.via(
                    SimpleFunction(lambda iterable: iterable)  # Optimized by LLM: Renamed parameter for clarity
                )
            ).setCoder(elemCoder)