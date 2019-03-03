
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


def newetl():
    query = """
        with d as
        (
             --add an extra column named main date which is the 
             --converted date format of event time column
             select *, TO_TIMESTAMP(event_time, 'dd/mm/yyyy HH24:MI:SS') main_date
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
    sourcetable = fromdb(fromconnection, query)

    print 'sourcetable is ready to insert'
    print 'inserting data to the destination table......'
    appenddb(sourcetable, toconnection, 'dashboard_fact')

    print 'insertion completed'
    # return sourcetable


