import psycopg2
from config import redshift_config

class Load:

    conn = None
    cur = None

    def connect(self):
        """ Connect to the PostgreSQL database server """
        try:
            # read connection parameters
            params = redshift_config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**params)

            # create a cursor
            self.cur = self.conn.cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def insert(self, values):
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
        "relationship_with_parent",
         "id"
        )
        fields = ', '.join(fields)
        postgres_insert_query = "INSERT INTO dashboard_facttabledemo ({0}) VALUES {1}".format(fields, values)
        print postgres_insert_query
        self.cur.execute(postgres_insert_query)
        self.conn.commit()
        count = self.cur.rowcount
        print (count, "Record inserted successfully into mobile table")

        if self.conn:
            self.cur.close()
            self.conn.close()
            print("PostgreSQL connection is closed")
