import psycopg2
from config import redshift_config
import datetime

class Load:
    """
    this is load class
    this class is responsible for loading data to the fact table
    """

    destination_db_connection = None
    cursor = None

    def connect(self):
        """ Connect to the PostgreSQL database server """
        try:
            # read connection parameters
            params = redshift_config()

            # connect to the PostgreSQL server
            print('Connecting to the redshift database...')
            self.destination_db_connection  = psycopg2.connect(**params)

            # create a cursor
            self.cursor = self.destination_db_connection .cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def insert(self, facts):
        """
        This method takes list of touples with field values
        for fact table and inserts these values into the fact table
        :param facts:
        :return:
        """
        #connect to destination database
        self.connect()

        count = 0
        try:
            print 'Loading data to the destination database...'
            for fact in facts:
                count = count+1
                #execute insert query
                self.cursor.execute('INSERT INTO  dashboard_fact (membership_no, first_free_join_date, '
                                 'first_piad_subscription_date, last_piad_subscription_date, last_free_subscription_date, '
                                 'number_of_renew_performed, number_of_paid_subscription, first_retailer_or_channel, '
                                 'avg_membership_duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',(fact))

            #change to the database
            self.destination_db_connection.commit()

            print (count, "Record inserted successfully into mobile table")

            if self.destination_db_connection:
                self.cursor.close()
                self.destination_db_connection.close()
                print("redshift database  connection is closed")

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

