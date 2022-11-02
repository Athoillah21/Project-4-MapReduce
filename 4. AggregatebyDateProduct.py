from mrjob.job import MRJob
from mrjob.step import MRStep
import psycopg2

class AggregateDate(MRJob):

    def reducer_init(self):
        self.conn = psycopg2.connect(database='postgres', user='athok', password='password', host='localhost', port='1234')

    def first_mapper(self, _, line):
        item = line.strip().split(',')
        yield item[1], int(item[4])

    def first_reducer(self, key, values):
        self.cur = self.conn.cursor()
        self.cur.execute("insert into agregatedateproduct (purchase_date, total_order) values(%s,%s)", (key, sum(values)))
        self.conn.commit()

    def first_store(self, key, values):
        self.cur = self.conn.cursor()
        self.cur.execute("insert into agregatedateproduct (purchase_date, total_order) values(%s,%s)", (key, values))
        self.conn.commit()

    def second_mapper(self, _, line):
        item = line.strip().split(',')
        yield item[3], int(item[4])

    def second_reducer(self, key, values):
        self.cur = self.conn.cursor()
        self.cur.execute("insert into agregatedateproduct (product, total_order) values(%s,%s)", (key, sum(values)))
        self.conn.commit()

    def second_store(self, key, values):
        self.cur = self.conn.cursor()
        self.cur.execute("insert into agregatedateproduct (product, total_order) values(%s,%s)", (key, values))
        self.conn.commit()
        self.conn.close()

    def steps(self):
        return [
            MRStep(mapper=self.first_mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.first_reducer),
            MRStep(reducer=self.first_store),
            MRStep(mapper=self.second_mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.second_reducer),
            MRStep(reducer=self.second_store),
                   ]
if __name__ == '__main__':
    AggregateDate.run()






















# class AggregateDateProduct(MRJob):

#     def reducer_init(self):
#         self.conn = psycopg2.connect(database='postgres', user='athok', password='password', host='localhost', port='1234')

#     def mapper_date(self, _, line):
#         item = line.strip().split(',')
#         self.increment_counter('First Job', '1. Split Data', 1)
#         yield item[1]
#         # , int(item[4])

#     def reducer_date(self, key, values):
#         self.cur = self.conn.cursor()
#         self.cur.execute("insert into agregatedateproduct (purchase_date) values(%s)", (key))
#         self.increment_counter('First Job', '2. Sum total order per purchase date', 1)
#         self.conn.commit()   

#     # def store_date(self, key, values):
#     #     self.cur = self.conn.cursor()
#     #     self.cur.execute("insert into agregatedateproduct (purchase_date, total_order) values(%s,%s)", (key, values))
#     #     self.increment_counter('First Job', '3. Store Data to Postgres', 1)
#     #     self.conn.commit()
#     #     self.conn.close()

#     def mapper_product(self, _, line):
#         item = line.strip().split(',')
#         self.increment_counter('Second Job', '1. Split Data', 1)
#         yield item[3], int(item[4])

#     def reducer_merger(self, key, values):
#         self.cur = self.conn.cursor()
#         self.cur.execute("select o.order_date, p.product_name, o.quantity from public.product as p inner join public.order as o ON p.product_id = o.product_id values(%s,%s)", (key, values))
#         self.increment_counter('Second Job', '2. Sum total order per purchase date and products', 1)
#         self.conn.commit()

#     def reducer_product(self, key, values):
#         self.cur = self.conn.cursor()
#         self.cur.execute("insert into agregatedateproduct (product, total_order) values(%s,%s)", (key, sum(values)))
#         self.increment_counter('Second Job', '3. Sum total order per purchase date and product', 1)
#         self.conn.commit()     

#     def store_all(self, key, values):
#         self.cur = self.conn.cursor()
#         self.cur.execute("insert into agregatedate (product, total_order) values(%s,%s)", (key, values))
#         self.increment_counter('Second Job', '4. Store Data to Postgres', 1)
#         self.conn.commit()
#         self.conn.close()

#     def steps(self):
#         return [
#             MRStep(mapper=self.mapper_date,
#                    reducer_init=self.reducer_init,
#                    reducer=self.reducer_date),
#             MRStep(mapper=self.mapper_product,
#                    reducer_init=self.reducer_init,
#                    combiner=self.reducer_merger,
#                    reducer=self.store_all)
#                    ]
# if __name__ == '__main__':
#     AggregateDateProduct.run()