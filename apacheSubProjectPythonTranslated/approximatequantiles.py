from typing import Any, List, TypeVar, Generic, Collection, Iterator, Optional

import apache_beam as beam

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')
ComparatorT = TypeVar('ComparatorT')

class ApproximateQuantiles:
    def __init__(self):
        pass

    @staticmethod
    def globally(numQuantiles: int, compareFn: ComparatorT) -> beam.PTransform:
        return beam.Combine.globally(ApproximateQuantiles.ApproximateQuantilesCombineFn.create(numQuantiles, compareFn))

    @staticmethod
    def globallyComparable(numQuantiles: int) -> beam.PTransform:
        return beam.Combine.globally(ApproximateQuantiles.ApproximateQuantilesCombineFn.create(numQuantiles))

    @staticmethod
    def perKey(numQuantiles: int, compareFn: ComparatorT) -> beam.PTransform:
        return beam.Combine.perKey(ApproximateQuantiles.ApproximateQuantilesCombineFn.create(numQuantiles, compareFn))

    @staticmethod
    def perKeyComparable(numQuantiles: int) -> beam.PTransform:
        return beam.Combine.perKey(ApproximateQuantiles.ApproximateQuantilesCombineFn.create(numQuantiles))

    class ApproximateQuantilesCombineFn(Generic[T, ComparatorT]):
        DEFAULT_MAX_NUM_ELEMENTS = int(1e9)

        def __init__(self, numQuantiles: int, compareFn: ComparatorT, bufferSize: int, numBuffers: int, maxNumElements: int):
            checkArgument(numQuantiles >= 2)
            checkArgument(bufferSize >= 2)
            checkArgument(numBuffers >= 2)
            self.numQuantiles = numQuantiles
            self.compareFn = compareFn
            self.bufferSize = bufferSize
            self.numBuffers = numBuffers
            self.maxNumElements = maxNumElements

        @staticmethod
        def create(numQuantiles: int, compareFn: ComparatorT) -> 'ApproximateQuantiles.ApproximateQuantilesCombineFn':
            return ApproximateQuantiles.ApproximateQuantilesCombineFn.create(numQuantiles, compareFn, ApproximateQuantiles.ApproximateQuantilesCombineFn.DEFAULT_MAX_NUM_ELEMENTS, 1.0 / numQuantiles)

        @staticmethod
        def createComparable(numQuantiles: int) -> 'ApproximateQuantiles.ApproximateQuantilesCombineFn':
            return ApproximateQuantiles.ApproximateQuantilesCombineFn.create(numQuantiles, Top.Natural())

        def withEpsilon(self, epsilon: float) -> 'ApproximateQuantiles.ApproximateQuantilesCombineFn':
            return ApproximateQuantiles.ApproximateQuantilesCombineFn.create(self.numQuantiles, self.compareFn, self.maxNumElements, epsilon)

        @staticmethod
        def create(numQuantiles: int, compareFn: ComparatorT, maxNumElements: int, epsilon: float) -> 'ApproximateQuantiles.ApproximateQuantilesCombineFn':
            b = 2
            while (b - 2) * (1 << (b - 2)) < epsilon * maxNumElements:
                b += 1
            b -= 1
            k = max(2, int((maxNumElements / (1 << (b - 1))) + 0.999999))
            return ApproximateQuantiles.ApproximateQuantilesCombineFn(numQuantiles, compareFn, k, b, maxNumElements)

        def createAccumulator(self) -> 'ApproximateQuantiles.QuantileState':
            return ApproximateQuantiles.QuantileState.empty(self.compareFn, self.numQuantiles, self.numBuffers, self.bufferSize)

        def getAccumulatorCoder(self, registry: Any, elementCoder: Any) -> Any:
            return ApproximateQuantiles.QuantileStateCoder(self.compareFn, elementCoder)

        def populateDisplayData(self, builder: Any) -> None:
            super().populateDisplayData(builder)
            builder.add(DisplayData.item("numQuantiles", self.numQuantiles).withLabel("Quantile Count"))
            builder.add(DisplayData.item("comparer", self.compareFn.__class__).withLabel("Record Comparer"))

        def getNumBuffers(self) -> int:
            return self.numBuffers

        def getBufferSize(self) -> int:
            return self.bufferSize

    class QuantileState(Generic[T, ComparatorT]):
        def __init__(self, compareFn: ComparatorT, numQuantiles: int, min: Optional[T], max: Optional[T], numBuffers: int, bufferSize: int, unbufferedElements: Collection[T], buffers: Collection['ApproximateQuantiles.QuantileBuffer']):
            self.compareFn = compareFn
            self.numQuantiles = numQuantiles
            self.numBuffers = numBuffers
            self.bufferSize = bufferSize
            self.min = min
            self.max = max
            self.buffers = sorted(buffers, key=lambda x: x.level)
            self.unbufferedElements = list(unbufferedElements)

        @staticmethod
        def empty(compareFn: ComparatorT, numQuantiles: int, numBuffers: int, bufferSize: int) -> 'ApproximateQuantiles.QuantileState':
            return ApproximateQuantiles.QuantileState(compareFn, numQuantiles, None, None, numBuffers, bufferSize, [], [])

        def addInput(self, elem: T) -> None:
            if self.isEmpty():
                self.min = self.max = elem
            elif self.compareFn.compare(elem, self.min) < 0:
                self.min = elem
            elif self.compareFn.compare(elem, self.max) > 0:
                self.max = elem
            self.addUnbuffered(elem)

        def addUnbuffered(self, elem: T) -> None:
            self.unbufferedElements.append(elem)
            if len(self.unbufferedElements) == self.bufferSize:
                self.unbufferedElements.sort(key=self.compareFn)
                self.buffers.append(ApproximateQuantiles.QuantileBuffer(self.unbufferedElements))
                self.unbufferedElements = []
                self.collapseIfNeeded()

        def mergeAccumulator(self, other: 'ApproximateQuantiles.QuantileState') -> None:
            if other.isEmpty():
                return
            if self.min is None or self.compareFn.compare(other.min, self.min) < 0:
                self.min = other.min
            if self.max is None or self.compareFn.compare(other.max, self.max) > 0:
                self.max = other.max
            for elem in other.unbufferedElements:
                self.addUnbuffered(elem)
            self.buffers.extend(other.buffers)
            self.collapseIfNeeded()

        def isEmpty(self) -> bool:
            return not self.unbufferedElements and not self.buffers

        def collapseIfNeeded(self) -> None:
            while len(self.buffers) > self.numBuffers:
                toCollapse = [self.buffers.pop(0), self.buffers.pop(0)]
                minLevel = toCollapse[1].level
                while self.buffers and self.buffers[0].level == minLevel:
                    toCollapse.append(self.buffers.pop(0))
                self.buffers.append(self.collapse(toCollapse))

        def collapse(self, buffers: List['ApproximateQuantiles.QuantileBuffer']) -> 'ApproximateQuantiles.QuantileBuffer':
            newLevel = 0
            newWeight = 0
            for buffer in buffers:
                newLevel = max(newLevel, buffer.level + 1)
                newWeight += buffer.weight
            newElements = self.interpolate(buffers, self.bufferSize, newWeight, self.offset(newWeight))
            return ApproximateQuantiles.QuantileBuffer(newLevel, newWeight, newElements)

        def offset(self, newWeight: int) -> int:
            if newWeight % 2 == 1:
                return (newWeight + 1) // 2
            else:
                self.offsetJitter = 2 - self.offsetJitter
                return (newWeight + self.offsetJitter) // 2

        def interpolate(self, buffers: Iterable['ApproximateQuantiles.QuantileBuffer'], count: int, step: float, offset: float) -> List[T]:
            iterators = [buffer.sizedIterator() for buffer in buffers]
            sortedIter = mergeSorted(iterators, lambda a, b: self.compareFn.compare(a.getValue(), b.getValue()))

            newElements = []
            weightedElement = next(sortedIter)
            current = weightedElement.getWeight()
            for j in range(count):
                target = j * step + offset
                while current <= target and sortedIter.hasNext():
                    weightedElement = next(sortedIter)
                    current += weightedElement.getWeight()
                newElements.append(weightedElement.getValue())
            return newElements

        def extractOutput(self) -> List[T]:
            if self.isEmpty():
                return []
            totalCount = len(self.unbufferedElements)
            for buffer in self.buffers:
                totalCount += self.bufferSize * buffer.weight
            allBuffers = self.buffers.copy()
            if self.unbufferedElements:
                self.unbufferedElements.sort(key=self.compareFn)
                allBuffers.append(ApproximateQuantiles.QuantileBuffer(self.unbufferedElements))
            step = totalCount / (self.numQuantiles - 1)
            offset = (totalCount - 1) / (self.numQuantiles - 1)
            quantiles = self.interpolate(allBuffers, self.numQuantiles - 2, step, offset)
            quantiles.insert(0, self.min)
            quantiles.append(self.max)
            return quantiles

    class QuantileBuffer(Generic[T]):
        def __init__(self, elements: List[T]):
            self.level = 0
            self.weight = 1
            self.elements = elements

        def __str__(self) -> str:
            return f"QuantileBuffer[level={self.level}, weight={self.weight}, elements={self.elements}]"

        def sizedIterator(self) -> Iterator['WeightedValue']:
            return (WeightedValue.of(elem, self.weight) for elem in self.elements)

    class QuantileStateCoder(Generic[T, ComparatorT]):
        def __init__(self, compareFn: ComparatorT, elementCoder: Any):
            self.compareFn = compareFn
            self.elementCoder = elementCoder
            self.elementListCoder = ListCoder.of(elementCoder)
            self.intCoder = BigEndianIntegerCoder()

        def encode(self, state: 'ApproximateQuantiles.QuantileState', outStream: Any) -> None:
            self.intCoder.encode(state.numQuantiles, outStream)
            self.intCoder.encode(state.bufferSize, outStream)
            self.elementCoder.encode(state.min, outStream)
            self.elementCoder.encode(state.max, outStream)
            self.elementListCoder.encode(state.unbufferedElements, outStream)
            self.intCoder.encode(len(state.buffers), outStream)
            for buffer in state.buffers:
                self.encodeBuffer(buffer, outStream)

        def decode(self, inStream: Any) -> 'ApproximateQuantiles.QuantileState':
            numQuantiles = self.intCoder.decode(inStream)
            bufferSize = self.intCoder.decode(inStream)
            min = self.elementCoder.decode(inStream)
            max = self.elementCoder.decode(inStream)
            unbufferedElements = self.elementListCoder.decode(inStream)
            numBuffers = self.intCoder.decode(inStream)
            buffers = [self.decodeBuffer(inStream) for _ in range(numBuffers)]
            return ApproximateQuantiles.QuantileState(self.compareFn, numQuantiles, min, max, numBuffers, bufferSize, unbufferedElements, buffers)

        def encodeBuffer(self, buffer: 'ApproximateQuantiles.QuantileBuffer', outStream: Any) -> None:
            outData = DataOutputStream(outStream)
            outData.writeInt(buffer.level)
            outData.writeLong(buffer.weight)
            self.elementListCoder.encode(buffer.elements, outStream)

        def decodeBuffer(self, inStream: Any) -> 'ApproximateQuantiles.QuantileBuffer':
            inData = DataInputStream(inStream)
            return ApproximateQuantiles.QuantileBuffer(inData.readInt(), inData.readLong(), self.elementListCoder.decode(inStream))

        def registerByteSizeObserver(self, state: 'ApproximateQuantiles.QuantileState', observer: Any) -> None:
            self.elementCoder.registerByteSizeObserver(state.min, observer)
            self.elementCoder.registerByteSizeObserver(state.max, observer)
            self.elementListCoder.registerByteSizeObserver(state.unbufferedElements, observer)

            self.intCoder.registerByteSizeObserver(len(state.buffers), observer)
            for buffer in state.buffers:
                observer.update(4 + 8)
                self.elementListCoder.registerByteSizeObserver(buffer.elements, observer)

        def equals(self, other: Optional[Any]) -> bool:
            if other is self:
                return True
            if not isinstance(other, ApproximateQuantiles.QuantileStateCoder):
                return False
            return self.elementCoder == other.elementCoder and self.compareFn == other.compareFn

        def hashCode(self) -> int:
            return hash((self.elementCoder, self.compareFn))

        def verifyDeterministic(self) -> None:
            verifyDeterministic(self, "QuantileState.ElementCoder must be deterministic", self.elementCoder)
            verifyDeterministic(self, "QuantileState.ElementListCoder must be deterministic", self.elementListCoder)