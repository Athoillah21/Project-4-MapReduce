import pandas as pd
import psycopg2
import sqlalchemy

data_product = pd.read_csv('dataset/TR_Products.csv')
data_user = pd.read_csv('dataset/TR_UserInfo.csv')
data_order = pd.read_csv('dataset/TR_OrderDetails.csv')

col_p = {"ProductID":"product_id", "ProductName":"product_name", "ProductCategory":"product_category", "Price":"price"}
col_u = {"UserID":"user_id", "UserSex":"user_sex", "UserDevice":"user_device"}
col_o = {"OrderID":"order_id", "OrderDate":"order_date", "PropertyID":"property_id", "ProductID":"product_id", "Quantity":"quantity"}

data_product = data_product.rename(columns=col_p)
data_user = data_user.rename(columns=col_u)
data_order = data_order.rename(columns=col_o)

conn = sqlalchemy.create_engine(url='postgresql://athok:password@localhost:1234/postgres')

data_product.to_sql('product', con=conn, index=False, if_exists='replace')
data_user.to_sql('user', con=conn, index=False, if_exists='replace')
data_order.to_sql('order', con=conn, index=False, if_exists='replace')