import pandas as pd
import numpy as np
import os
from datetime import datetime


df = pd.read_csv(r"Z:\Power BI\Projects\Data Analyst Project - SQL+PowerBI+Excel\pizza_sales.csv")


"""
Functions
"""

def unique_values(feature_name):
    unique_value = pd.unique(df.loc[:, feature_name])
    result_df = pd.DataFrame({feature_name: unique_value} )
    return result_df

def remove_duplicates(feature_list):
    result_df = df.loc[:, feature_list]
    result_df = result_df.drop_duplicates()
    return result_df

"""
Creating tables
"""

# size, category, pizza_ingredients df
features_list = ['pizza_size', 'pizza_category', 'pizza_ingredients']
dfs = {}
for feature in features_list:
    dfs['{}_df'.format(feature)] = unique_values(feature)
    


# modify ingredient_df 
#   each row recored a unqiue ingredient
#   remove duplicates
#   handle inconsistency data - Artichoke VS Artichokes


ingredients_df = dfs['pizza_ingredients_df']
ingredients = ingredients_df['pizza_ingredients']
ingredients_str = ', '.join(ingredients)
ingredient_list_0 = ingredients_str.split(', ')
ingredient_list = pd.unique(ingredient_list_0)
ingredient_list.sort()
ingredients_ele_df = pd.DataFrame({'pizza_ingredient': ingredient_list})
ingredients_ele_df.set_index('pizza_ingredient', inplace = True)
ingredients_ele_df.drop(index = 'Artichokes', axis = 0, inplace = True)
ingredients_ele_df.reset_index(inplace = True)
dfs['ingredient_ele_df'] = ingredients_ele_df




# pizza, pizza_and_ingredients df

feature_lists = [['pizza_name', 'pizza_category'],  # pizza
                 ['pizza_name', 'pizza_ingredients']] # pizza & ingredients


dfs['pizza_df'] = remove_duplicates(feature_lists[0])


# break down ingredient lists, for each type of pizza, ensure each row only contains one ingredient
dfs['pizza_and_ingredients_df'] = remove_duplicates(feature_lists[1])
pi_df = dfs['pizza_and_ingredients_df']


pizza_ingredients_list = pi_df.to_numpy()
pizza_and_ingredients_dfs = []
for p in range(len(pizza_ingredients_list)):
    ingredient_list = pizza_ingredients_list[p][1].split(', ')
    pizza_name_list = [pizza_ingredients_list[p][0]] * len(ingredient_list)
    df_pi = pd.DataFrame({'pizza': pizza_name_list, 'ingredient_list': ingredient_list})
    pizza_and_ingredients_dfs.append(df_pi)
pizza_and_ingredients_df = pd.concat(pizza_and_ingredients_dfs)



# replace inconsistent data
# Artichoke Vs Artichokes
pizza_and_ingredients_df.loc[(pizza_and_ingredients_df['ingredient_list'] == 'Artichokes'), 'ingredient_list'] = 'Artichoke'


dfs['pizza_and_ingredients_df'] = pizza_and_ingredients_df


# orders, sales df
aggregate_features = [['order_id', 'order_date', 'order_time', 'total_price'],
                      ['pizza_name', 'order_id', 'pizza_size', 'unit_price', 'quantity']]
                      
df_names =['order', 'sale']

# order 
result_df_orders = df.loc[:, aggregate_features[0]]
result_df_orders = result_df_orders.groupby(['order_id', 'order_date', 'order_time']).sum()
result_df_orders.reset_index(inplace = True)
result_df_orders['order_date'] = pd.to_datetime(result_df_orders['order_date'], format = "%d-%m-%Y")
result_df_orders['order_time'] = pd.to_datetime(result_df_orders['order_time'], format = "%H:%M:%S").dt.time

dfs['order_df'] = result_df_orders


# sales 
result_df_sales = df.loc[:, aggregate_features[1]]
result_df_sales = result_df_sales.groupby(['pizza_name', 'order_id', 'pizza_size', 'unit_price']).sum()
result_df_sales.reset_index(inplace = True)
dfs['sale_df'] = result_df_sales


"""
    add ids (pizza, size, categories, ingredients)
"""

id_df_list = ['pizza', 'pizza_size', 'pizza_category', 'ingredient_ele']
sort_columns = ['pizza_name', 'pizza_size', 'pizza_category', 'pizza_ingredient']
id_col_name = ['pizza_id', 'size_id', 'category_id', 'ingredient_id']

for i in range(len(id_df_list)):
    tem_df = dfs['{}_df'.format(id_df_list[i])]
    tem_df.sort_values(inplace = True, by = sort_columns[i])
    tem_df[id_col_name[i]] = range(1, tem_df.shape[0]+1)
    # dfs['{}_df'.format(id_df_list[i])] = tem_df




"""
replace values with ids (pizzas[category], Sales[pizza, size])
"""
value2id_list = ['pizza_df', 'sale_df', 'sale_df','pizza_and_ingredients_df', 'pizza_and_ingredients_df' ]
ref_df_list  = ['pizza_category', 'pizza_size', 'pizza', 'pizza', 'ingredient_ele']
column_names = ['pizza_category', 'pizza_size', 'pizza_name', 'pizza', 'ingredient_list']
ref_list = []
columns_rename = ['category_id', 'size_id', 'pizza_id', 'pizza_id', 'ingredient_id']

for j in range(len(ref_df_list)):
    ref_df = dfs['{}_df'.format(ref_df_list[j])]

    if ref_df.shape[1] > 2:
        ref_list.append(ref_df.loc[:,['pizza_name', 'pizza_id']].to_numpy())
        
    else:
        ref_list.append(ref_df.to_numpy())


   
for i in range(len(value2id_list)):
    ref_tab = ref_list[i]
    tem_id_df = dfs[value2id_list[i]] # df need to be modified
    for ele in ref_tab:
        tem_id_df.loc[tem_id_df[column_names[i]] == ele[0], column_names[i]] = ele[1]

    tem_id_df.rename(columns = {column_names[i]: columns_rename[i]}, inplace = True)
    # print(value2id_list[i], column_names[i], columns_rename[i])


    
del dfs['pizza_ingredients_df']


keys = dfs.keys()
path = r"Z:\Power BI\Projects\Data Analyst Project - SQL+PowerBI+Excel\data\{}.csv"
for key in keys:
    dfs[key].to_csv(path.format(key), index = False)




