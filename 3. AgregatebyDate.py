from mrjob.job import MRJob
from mrjob.step import MRStep
import psycopg2

class AggregateDate(MRJob):

    def reducer_init(self):
        self.conn = psycopg2.connect(database='postgres', user='athok', password='password', host='localhost', port='1234')

    def mapper(self, _, line):
        item = line.strip().split(',')
        self.increment_counter('First Job', '1. Split Data', 1)
        yield item[1], int(item[4])

    def reducer(self, key, values):
        self.cur = self.conn.cursor()
        self.cur.execute("insert into agregatedate (purchase_date, total_order) values(%s,%s)", (key, sum(values)))
        self.increment_counter('First Job', '2. Sum total order per purchase date', 1)
        self.conn.commit()     

    def store(self, key, values):
        self.cur = self.conn.cursor()
        self.cur.execute("insert into agregatedate (purchase_date, total_order) values(%s,%s)", (key, values))
        self.increment_counter('Second Job', '1. Store Data to Postgres', 1)
        self.conn.commit()
        self.conn.close()

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer),
            MRStep(reducer=self.store)
                   ]
if __name__ == '__main__':
    AggregateDate.run()