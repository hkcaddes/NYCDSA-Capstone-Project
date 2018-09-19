# test redshift connection

import psycopg2



con = psycopg2.connect(dbname='dev', host='redshift-cluster-1.cah6qt0iybst.us-east-2.redshift.amazonaws.com', port=5439, user='awsuser', password='Capstone1')

cur = con.cursor()

#cur.execute("ROLLBACK;")

cur.execute("SELECT * FROM tickets_splits LIMIT 10;")

cur.fetchall()




# load data into numpy
import numpy as np
data = np.array(cur.fetchall())
type(cur.fetchall())

cur.close()
con.close()

# load data into pandas
import sqlalchemy_redshift
from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy_redshift.dialect

engine = create_engine('redshift+psycopg2://redshift-cluster-1.cah6qt0iybst.us-east-2.redshift.amazonaws.com:5439/dev')
