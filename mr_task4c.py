from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    def steps(self):
        return[
            MRStep (mapper=self.mapper,
            reducer=self.reducer),
            MRStep (reducer=self.sort_by_count)
        ]
    # The mapper produces a list of (key, value)
    # key is the payment type and value is the count.
    def mapper(self, _, line):
        vendor = line.strip()
        vendor = vendor.split(",")
        if (vendor[0] != "VendorID"): #ignore header
            yield (vendor[9], 1) #payment type and count

    # The reducer produces one instance for each payment type and its count.
    def reducer(self, paymenttype, count):
        yield None, (sum(count),  paymenttype)
    
    # The sort_by_count produces one instance for each payment type and its count in sorted order.
    def sort_by_count(self, _, pair):
        sorted_pairs = sorted(pair, reverse=True)
        for pair in sorted_pairs:
            yield pair



if __name__ == '__main__':
    MyMapReduce.run()

