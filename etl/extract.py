import time
import psycopg2

from petl import fromdb, todb, appenddb, look

from config import db_config, redshift_config
from load import Load


QUERY = """
        with d as
        (
             --add an extra column named main date which is the 
             --converted date format of event time column
             select *, TO_TIMESTAMP(event_time, 'dd/mm/yyyy HH24:MI:SS')::timestamp without time zone main_date
             from subscription_data
             where membership_no is not null

        ),
         c0 as 
         (
            --take all the rows which membership_no is not null
            --this is the base table with which all the other query
            --tables will be joined
             select membership_no
             from subscription_data
             where membership_no is not null
             group by membership_no
             limit 10
        ) ,
        c1 as
        (   
             --takes rows with membership_no and
             --takes min date and max date which contains tonic basic as current_product_code or 
             --previous_product_code. min date is first join date and max date is last free subscription
             --or last subscription as tonic basic from table d
             select membership_no,min(main_date) "first_free_join_date", max(main_date) "last_free_subscription_date"
             from d
             where current_product_code = 'TonicBasic' or previous_product_code = 'TonicBasic'
             group by membership_no
        ),
        c2 as
        (
             --takes rows with membership_no and
             --min date of transaction_id(means paid subscription starts)
             --max date of transaction_id(means last paid subscription starts)
             --from table d
             select membership_no,min(main_date) "first_piad_subscription_date", max(main_date) "last_piad_subscription_date"
             from d
             where transaction_id is not null --current_product_code not in ('TonicBasic') --or previous_product_code = 'TonicBasic'
             group by membership_no
        ),

        c3 as
        (
             --takes rows with membership_no and 
             --number of renew a member performed
             --from table d
             select membership_no,count(*) "number_of_renew_performed"
             from d
             where subscription_status = 'RENEW'
             group by membership_no
        ),

        c4 as
        (
             --takes rows with membership_no and 
             --total number of paid subscription a member availed(means total transaction_id)
             --from table d
             select membership_no,count(*) "number_of_paid_subscription"
             from d
             where transaction_id is not null
             group by membership_no
        ),

        c5 as
        (
             --takes rows with membership_no and 
             --added a rnk column which will be partitioned by(number of entries) membership_no
             --and will be ordered by main_date field
             --from d table
             select membership_no,subscription_channel "first_retailer_or_channel"
             from (select membership_no,
                          subscription_channel,
                          row_number() over (partition by membership_no order by main_date) rnk
                   from d
                   where subscription_channel is not null) b where rnk =1
        ),

        c6 as
        (
             --takes rows with membership_no and 
             --average duration of membership of a member
             --here if a member unsubscribes then also he obtains a free basis membership
             --so average duration is  (last date-join date)/number_of_durations 

             select membership_no, DATE_PART('day', "Last Free Subscription" - "First Join Date") / (kount * 1.0) "avg_membership_duration"
             from (
             select membership_no,min(main_date) "First Join Date", max(main_date) "Last Free Subscription", count(*) kount
             from d
             group by membership_no)b
        )

        select *
        from (
        select c0.membership_no, 
        c1."first_free_join_date", 
        c2."first_piad_subscription_date", 
        c2."last_piad_subscription_date", 
        c1."last_free_subscription_date",
        c3."number_of_renew_performed",
        c4."number_of_paid_subscription",
        c5."first_retailer_or_channel",
        c6."avg_membership_duration"
        from c0 left join c1 on c0.membership_no = c1.membership_no
                left join c2 on c0.membership_no = c2.membership_no
                left join c3 on c0.membership_no = c3.membership_no
                left join c4 on c0.membership_no = c4.membership_no
                left join c5 on c0.membership_no = c5.membership_no
                left join c6 on c0.membership_no = c6.membership_no
        ) r """


class Extract:
    """
    This is a custom ETL class
    just call etl() method of this class and this
    method performs etl from source databse to destination
    database
    """

    subscriptions = None
    source_db_connection = None
    cursor = None

    def connect(self):
        """ Connect to the PostgreSQL database server """
        try:
            # read connection parameters
            params = db_config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.source_db_connection= psycopg2.connect(**params)

            # create a cursor
            self.cursor = self.source_db_connection.cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def etl(self):
        """
        this is the main etl method
        :return:
        """
        self.connect()
        try:
            print 'data extraction started...'
            start_time = time.time()
            self.cursor.execute(QUERY)
            end_time = time.time()
            print 'total for extracting data={} s'.format(end_time - start_time)
            print 'data extraction completed'

            self.subscriptions = self.cursor.fetchall()

            # close the communication with the PostgreSQL
            self.cursor.close()


        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self.source_db_connection is not None:
                self.source_db_connection.close()
                print('Source database connection closed.')
            self.load()

    def load(self):
        loader = Load()
        loader.insert(self.subscriptions)


def etl2():
    """
    this etl method performs etl based of petl library
    :return:
    """
    print 'connecting to fromconnection...'
    fromconnection = psycopg2.connect(user="applicant", password="Applicant!23",
                                      host="th-data-test.ckvp0ck3llgr.ap-southeast-1.rds.amazonaws.com", port="5432",
                                      database="th_data_test")
    print 'connected to fromconnection'
    print 'connecting to toconnection...'

    toconnection = psycopg2.connect(user="applicant", password="Applicant!23",
                                    host="th-data-test.ccu6ybiolunl.ap-southeast-1.redshift.amazonaws.com", port="5439",
                                    database="th_data_test")
    print 'connected to toconnection'

    print 'fetching data from source.....'
    sttime = time.time()
    sourcetable = fromdb(fromconnection, QUERY)
    endtime = time.time()
    print 'total fetch time={0}'.format(endtime-sttime)

    print 'sourcetable is ready to insert'
    print 'inserting data to the destination table......'
    starttime = time.time()
    try:
        todb(sourcetable, toconnection, 'dashboard_fact')
    except Exception as e:
        print e
    endtime = time.time()
    print 'total time = {0}'.format(endtime-starttime)
    print 'insertion completed'


