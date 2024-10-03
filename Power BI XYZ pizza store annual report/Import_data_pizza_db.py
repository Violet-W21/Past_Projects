import glob
import pymysql
import pandas as pd

# coonect to database
mydb = pymysql.connect(
    host='localhost',
    user = 'root',
    password = 'wenxiaoyi589#',
    database= 'pizza_db'
)
#print(mydb)
my_cursor = mydb.cursor()



#    create tables + create relationships (pk, fk)

all_tables = ["category", "size", "ingredient", "pizza", "pizza_ingredient", "order", "sale"]
table_set = {"size", "category", "ingredient"}


create_table_sql_dict = {
# category, sizes, ingredients
"csi" : ( 
    "CREATE TABLE IF NOT EXISTS {} "
    "({}_id TINYINT PRIMARY KEY," 
    "{}_name VARCHAR(100))"
),

# pizzas
"pizza" : (
    "CREATE TABLE IF NOT EXISTS pizza "
    "(pizza_id TINYINT PRIMARY KEY, "
    "pizza_name VARCHAR(100), "
    "category_id TINYINT, "
    
    "FOREIGN KEY fk_pizza_category (category_id) "
    "REFERENCES category (category_id) "
    "ON UPDATE CASCADE "
    "ON DELETE SET NULL)"
    ),
                    
# orders
"order" : (
    "CREATE TABLE IF NOT EXISTS `order` "
    "(order_id MEDIUMINT PRIMARY KEY, "
    "order_date DATE, "
    "order_time TIME, "
    "total_price DECIMAL(5,2))"
    ),
    

# sales
"sale" : (
    "CREATE TABLE IF NOT EXISTS sale "
    "(pizza_id TINYINT NOT NULL, "
    "order_id MEDIUMINT NOT NULL, "
    "size_id TINYINT, "
    "unit_price DECIMAL(5,2), "
    "quantity TINYINT NOT NULL, "
    
    "FOREIGN KEY fk_pizza_sale (pizza_id) "
    "REFERENCES pizza (pizza_id) "
    "ON UPDATE CASCADE "
    "ON DELETE RESTRICT, "
    
    "FOREIGN KEY fk_order_sale (order_id) "
    "REFERENCES `order` (order_id) "
    "ON UPDATE CASCADE "
    "ON DELETE CASCADE, "
    
    "FOREIGN KEY fk_size_sale (size_id) "
    "REFERENCES size (size_id) "
    "ON UPDATE CASCADE "
    "ON DELETE RESTRICT)"
),

# pizza&ingredient
"pizza_ingredient" : (
    "CREATE TABLE IF NOT EXISTS pizza_ingredient "
    "(pizza_id TINYINT NOT NULL, "
    "ingredient_id TINYINT NOT NULL, "
    
    "FOREIGN KEY fk_pizza_ingredient_pizza (pizza_id) "
    "REFERENCES pizza (pizza_id) "
    "ON UPDATE CASCADE "
    "ON DELETE CASCADE, "
    
    "FOREIGN KEY fk_pizza_ingredient_ingredient (ingredient_id) "
    "REFERENCES ingredient (ingredient_id) "
    "ON UPDATE CASCADE "
    "ON DELETE CASCADE)"
)

}


# create tables in sql_db
for table in all_tables:
    if table in table_set:
        key_create = "csi"
        sql_table = create_table_sql_dict[key_create].format(table, table, table)
    else:
        key_create = table
        sql_table = create_table_sql_dict[key_create]
    
    my_cursor.execute(sql_table)



#my_cursor.execute("SHOW TABLES")
#for tb in my_cursor:
#    print(tb)

#drop_table_list = ["sale", "pizza_ingredient", "pizza", "size", "ingredient", "category", "`order`"]
#for table_drop in drop_table_list:
#   my_cursor.execute("DROP TABLE {}".format(table_drop))


# Import data into pizza_db
insert_table_sql_dict = {
    "csi" : (
    "INSERT INTO {} "
    "({}_name, {}_id) "
    "VALUES (%s, %s)"
    ), 

    "pizza" : (
        "INSERT INTO pizza "
        "(pizza_name, category_id, pizza_id) "
        "VALUES (%s, %s, %s)"
    ),

    "order" : (
        "INSERT INTO `order` "
        "(order_id, order_date, order_time, total_price) "
        "VALUES (%s, %s, %s, %s)"
    ),

    "sale" : (
        "INSERT INTO sale "
        "(pizza_id, order_id, size_id, unit_price, quantity) "
        "VALUES (%s, %s, %s, %s, %s)"
    ),

    "pizza_ingredient" : (
        "INSERT INTO pizza_ingredient "
        "(pizza_id, ingredient_id) "
        "VALUE (%s, %s)"
    )
} 

# read_files
path = r"Z:\Power BI\Projects\Data Analyst Project - SQL+PowerBI+Excel\data\{}_df.csv"

insert_order = ["pizza_category","pizza_size", "ingredient_ele", "pizza", "pizza_and_ingredients", "order", "sale"]
for file_name in insert_order:
    full_path = path.format(file_name)
    
    if file_name == 'ingredient_ele':
        file_name = 'pizza_ingredient'
        
    if file_name != 'pizza_and_ingredients':
        file_name_components_lst = file_name.split('_')
        if len(file_name_components_lst) == 2:
            file_name = file_name_components_lst[1]
        
    else: 
        file_name = "pizza_ingredient"

    
    curr_df = pd.read_csv(full_path)
    data_array = [tuple(row) for row in curr_df.to_numpy()] # convert each row form numpy array to tuple
    
    if file_name in table_set:
        key_insert = 'csi'
        sql_insert = insert_table_sql_dict['csi'].format(file_name, file_name, file_name)
    else:
        key_insert = file_name
        sql_insert = insert_table_sql_dict[key_insert]
    
    # insert data
    my_cursor.executemany(sql_insert, data_array)
    mydb.commit()
    #print("Successful! {}".format(file_name))
    