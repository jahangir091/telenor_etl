
import psycopg2
from config import db_config

from petl import todb


class Extract:

    subscriptions = None

    def connect(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = db_config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            # query = "select * from subscription_data where membership_no='T-1700103545-1'"
            query = "select * from subscription_data limit 10"
            cur.execute(query)

            # display the PostgreSQL database server version
            self.subscriptions = cur.fetchall()
            print cur.fetchall()
            print self.subscriptions


            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')



from petl import fromdb, todb, appenddb, look


def etl():
    fields = (
        "create_date",
        "subscription_on",
        "transaction_on",
        "event_time",
        "transaction_completed_on",
        "transaction_id",
        "membership_no",
        "previous_product_code",
        "current_product_code",
        "retailer_type",
        "retailer_area",
        "retailer_pos_code",
        "subscription_status",
        "user_type",
        "payment_type",
        "subscription_type",
        "subscription_channel",
        "transaction_channel",
        "user_type1",
        "parent_membership_no",
        "relationship_with_parent"
    )
    fields = ', '.join(fields)
    print 'establishing connection ...'
    fromconnection = psycopg2.connect(user="applicant", password="Applicant!23",
                                    host="th-data-test.ckvp0ck3llgr.ap-southeast-1.rds.amazonaws.com", port="5432",
                                    database="th_data_test")
    toconnection = psycopg2.connect(user="applicant", password="Applicant!23",
                                    host="th-data-test.ccu6ybiolunl.ap-southeast-1.redshift.amazonaws.com", port="5439",
                                    database="th_data_test")
    query = 'select {0}  from subscription_data limit 10'.format(fields)
    print query
    sourcetable = fromdb(fromconnection, query)

    print sourcetable

    print 'inserting data'
    appenddb(sourcetable, toconnection, 'dashboard_facttabledemo')

    print 'insertion completed'
