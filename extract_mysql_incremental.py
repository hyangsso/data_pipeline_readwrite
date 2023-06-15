import psycopg2
import configparser
import pymysql 
import csv
import boto3

# 1. 데이터 웨어하우스에서 가장 최신의 업로드 값을 찾는다. 

# 1-1. redshift 연결
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")

rs_conn = psycopg2.connect(
    "dbname=" + dbname
    + "user=" + user
    + "password=" + password    
    + "host=" + host
    + "port=" + port
    )

# 1-2. MAX를 이용하여 가장 최신의 업데이트 날짜를 추출
rs_sql = """SELECT COALESCE(MAX(LastUpdated),
        '1900-01-01')
        FROM Orders;
        """
        
rs_cursor = rs_conn.curosr()
rs_cursor.execute(rs_sql)
result = rs_cursor.fetchone()

# 1-3. 값 저장
last_updated_warehouse = result[0]

rs_cursor.close()
rs_conn.commit()

# 2. 소스 데이터에서 가장 최신 날짜 이후의 값을 추출한다. 

# 2-1. mysql 연결
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(host=hostname,
                    user=username,
                    password=password,
                    db=dbname,
                    port=int(port)
                    )

if conn is None:
    print("Error connecting to the MySQL database")
else: 
    print("MySQL connection established!")

# 2-2. 가장 최신 데이터 추출 (sql injection 방지 필수)
m_query = """SELECT * 
        FROM Orders
        WHERE LastUpdated > %s;
        """

# 2-3. csv 형태로 저장
local_filename = "order_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query, (last_updated_warehouse,))
results = m_cursor.fetchall()
with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)
    
fp.close()
m_cursor.close()
conn.close()

# 3. 데이터 웨어하우스에 로드하기 위한 결과 csv 파일을 s3 버킷에 업로드한다. 

# 3-1. s3 연결
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.client('s3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key)

s3_file = local_filename

# 3-2. s3에 업로드 
s3.upload_file(
    local_filename,
    bucket_name,
    s3_file
)