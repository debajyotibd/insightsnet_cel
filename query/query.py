import pandas as pd
import pandas as pd
import csv
from ccc import Corpora
from ccc import Corpus
import re
import os
import numpy as np


def word_query(registry_path:str, corpus_name:str , query:str , query_break:str = None , left_word:int = None , right_word:int = None , p_tag:list = "word", s_tag:list = ["text_id"]):
    
    corpus = Corpus(registry_path = registry_path, corpus_name = corpus_name)
    corpus


    #result_count = corpus.query('[word="let"%c]')


    result_count = corpus.query(f'[word="{query}"%c]',context_break = query_break, context_left = left_word, context_right = right_word)
    store = f"Total number of word {query} in this corpus are {result_count}."

    #########################################################################

    with open(f'{corpus_name}_with_search_{query}.txt', 'w') as f:
        print(store, file=f)

    read_file = pd.read_csv (f'{corpus_name}_with_search_{query}.txt', on_bad_lines='skip')
    read_file.to_csv (f'{corpus_name}_with_search_{query}.csv', index=None)

    #########################################################################

    col_names = ["col1"]
    df_read = read_file.to_csv (f'{corpus_name}_with_search_{query}.csv',index=None)
    df1 = pd.read_csv(f'{corpus_name}_with_search_{query}.csv', names=col_names)
    store_csv = df1.to_csv (f'{corpus_name}_with_search_{query}.csv', index=None)
    df1

    df2 = pd.DataFrame(df1)

    res = ''.join(df2['col1'])

            #res
    with open(f'{corpus_name}_single_line_out_word_{query}.txt', 'w') as f:
        print(res, file=f)  

    read_file = pd.read_csv (f'{corpus_name}_single_line_out_word_{query}.txt', on_bad_lines='skip')
    read_file.to_csv (f'{corpus_name}_single_line_out_word_{query}.csv', index=None)    

    col_names = ["col1"]
    #df_read = read_file.to_csv (r'single_line_out.csv',index=None)
    df3 = pd.read_csv(f"{corpus_name}_single_line_out_word_{query}.csv", names=col_names)
    store_csv = df3.to_csv (f'{corpus_name}_out_csv_single_word_{query}.csv', index=None)
    df3

    read_csv = pd.read_csv(f"{corpus_name}_out_csv_single_word_{query}.csv")
    read_csv

    new_file_splite = read_csv.col1.str.split(expand=True)
    store_csv_final = new_file_splite.to_csv (f'{corpus_name}_single_word_word_{query}.csv', index=None)
    store_csv_final

    read_csv_single = pd.read_csv(f"{corpus_name}_single_word_word_{query}.csv")
    read_csv_single

    #df_new = read_csv_single.filter(['0','1','2','3','4','5','6','7','8','9','10','11','12','13', '14','15'])
    df_new = read_csv_single.filter(['15'])

    df_new

    store_csv_single = df_new.to_csv (f'{corpus_name}_plotting_word_{query}.csv', header=False , index=False)
    store_csv_single

    data= pd.read_csv(f"{corpus_name}_plotting_word_{query}.csv")
    data

    #########################################################################

    headerList = ["Corpus"]
    with open(f"{corpus_name}_plot_word_{query}.csv", 'a') as file:
        dw = csv.DictWriter(file, delimiter=',', fieldnames=headerList)
        dw.writeheader()
        
    with open(f'{corpus_name}_plot_word_{query}.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow(data)

    data = pd.read_csv(f'{corpus_name}_plot_word_{query}.csv')
    df = pd.DataFrame(data)

    df = pd.read_csv(f'{corpus_name}_plot_word_{query}.csv')

    #########################################################################

    final = result_count.concordance( p_show = p_tag , s_show = s_tag , form = 'kwic', cut_off = None)
    final
    #final.to_csv("data_frame_query.csv", index = None)
    final.to_csv(f"{corpus_name}_data_frame_query_{query}.csv")

    #########################################################################
    num_res = pd.read_csv(f'{corpus_name}_plot_word_{query}.csv')
    df_res = pd.read_csv(f'{corpus_name}_data_frame_query_{query}.csv')

    pd.concat([num_res,df_res])

    #########################################################################

    cor = df.iloc[0, df.columns.get_loc("Corpus")]
    #th = df.iloc[0, df.columns.get_loc("Total_Hit")]

    #########################################################################

    df = pd.concat([num_res,df_res])
    #df = df.replace(np.nan, "EXAMPLE")
    df['Corpus'] = df["Corpus"].replace(np.nan, cor)
    #df['Total_Hit'] = df["Total_Hit"].replace(np.nan, th)

    #########################################################################

    df = df.iloc[1:]
    df.to_csv(f'{corpus_name}_{query}_output.csv', index = None)
    
    #########################################################################
    csv_files = [f for f in os.listdir() if f.endswith(f'_{query}.csv')]
    txt_files = [f for f in os.listdir() if f.endswith(f'_{query}.txt')]


    for i in csv_files:
        current_path = os.path.abspath(os.getcwd())
        file_path = f"{current_path}/{i}"
        os.remove(file_path)
        
    for j in txt_files:
        current_path = os.path.abspath(os.getcwd())
        file_path = f"{current_path}/{j}"
        os.remove(file_path)
    
    ########################################################################
    df_res1 = pd.read_csv(f'{corpus_name}_{query}_output.csv')
    return (df_res1)











def pos_query(registry_path:str, corpus_name:str , query:str , query_break:str = None , left_word:int = None , right_word:int = None , p_tag:list = "word", s_tag:list = ["text_id"]):
    
    corpus = Corpus(registry_path = registry_path, corpus_name = corpus_name)
    corpus


    #result_count = corpus.query('[word="let"%c]')


    result_count = corpus.query(f'[pos="{query}"%c]',context_break = query_break, context_left = left_word, context_right = right_word)
    store = f"Total number of word {query} in this corpus are {result_count}."

    #########################################################################

    with open(f'{corpus_name}_with_search_{query}.txt', 'w') as f:
        print(store, file=f)

    read_file = pd.read_csv (f'{corpus_name}_with_search_{query}.txt', on_bad_lines='skip')
    read_file.to_csv (f'{corpus_name}_with_search_{query}.csv', index=None)

    #########################################################################

    col_names = ["col1"]
    df_read = read_file.to_csv (f'{corpus_name}_with_search_{query}.csv',index=None)
    df1 = pd.read_csv(f'{corpus_name}_with_search_{query}.csv', names=col_names)
    store_csv = df1.to_csv (f'{corpus_name}_with_search_{query}.csv', index=None)
    df1

    df2 = pd.DataFrame(df1)

    res = ''.join(df2['col1'])

            #res
    with open(f'{corpus_name}_single_line_out_word_{query}.txt', 'w') as f:
        print(res, file=f)  

    read_file = pd.read_csv (f'{corpus_name}_single_line_out_word_{query}.txt', on_bad_lines='skip')
    read_file.to_csv (f'{corpus_name}_single_line_out_word_{query}.csv', index=None)    

    col_names = ["col1"]
    #df_read = read_file.to_csv (r'single_line_out.csv',index=None)
    df3 = pd.read_csv(f"{corpus_name}_single_line_out_word_{query}.csv", names=col_names)
    store_csv = df3.to_csv (f'{corpus_name}_out_csv_single_word_{query}.csv', index=None)
    df3

    read_csv = pd.read_csv(f"{corpus_name}_out_csv_single_word_{query}.csv")
    read_csv

    new_file_splite = read_csv.col1.str.split(expand=True)
    store_csv_final = new_file_splite.to_csv (f'{corpus_name}_single_word_word_{query}.csv', index=None)
    store_csv_final

    read_csv_single = pd.read_csv(f"{corpus_name}_single_word_word_{query}.csv")
    read_csv_single

    #df_new = read_csv_single.filter(['0','1','2','3','4','5','6','7','8','9','10','11','12','13', '14','15'])
    df_new = read_csv_single.filter(['15'])

    df_new

    store_csv_single = df_new.to_csv (f'{corpus_name}_plotting_word_{query}.csv', header=False , index=False)
    store_csv_single

    data= pd.read_csv(f"{corpus_name}_plotting_word_{query}.csv")
    data

    #########################################################################

    headerList = ["Corpus"]
    with open(f"{corpus_name}_plot_word_{query}.csv", 'a') as file:
        dw = csv.DictWriter(file, delimiter=',', fieldnames=headerList)
        dw.writeheader()
        
    with open(f'{corpus_name}_plot_word_{query}.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow(data)

    data = pd.read_csv(f'{corpus_name}_plot_word_{query}.csv')
    df = pd.DataFrame(data)

    df = pd.read_csv(f'{corpus_name}_plot_word_{query}.csv')

    #########################################################################

    final = result_count.concordance( p_show = p_tag , s_show = s_tag , form = 'kwic', cut_off = None)
    final
    #final.to_csv("data_frame_query.csv", index = None)
    final.to_csv(f"{corpus_name}_data_frame_query_{query}.csv")

    #########################################################################
    num_res = pd.read_csv(f'{corpus_name}_plot_word_{query}.csv')
    df_res = pd.read_csv(f'{corpus_name}_data_frame_query_{query}.csv')

    pd.concat([num_res,df_res])

    #########################################################################

    cor = df.iloc[0, df.columns.get_loc("Corpus")]
    #th = df.iloc[0, df.columns.get_loc("Total_Hit")]

    #########################################################################

    df = pd.concat([num_res,df_res])
    #df = df.replace(np.nan, "EXAMPLE")
    df['Corpus'] = df["Corpus"].replace(np.nan, cor)
    #df['Total_Hit'] = df["Total_Hit"].replace(np.nan, th)

    #########################################################################

    df = df.iloc[1:]
    df.to_csv(f'{corpus_name}_{query}_output.csv', index = None)
    
    #########################################################################
    csv_files = [f for f in os.listdir() if f.endswith(f'_{query}.csv')]
    txt_files = [f for f in os.listdir() if f.endswith(f'_{query}.txt')]


    for i in csv_files:
        current_path = os.path.abspath(os.getcwd())
        file_path = f"{current_path}/{i}"
        os.remove(file_path)
        
    for j in txt_files:
        current_path = os.path.abspath(os.getcwd())
        file_path = f"{current_path}/{j}"
        os.remove(file_path)
    
    ########################################################################
    df_res1 = pd.read_csv(f'{corpus_name}_{query}_output.csv')
    return (df_res1)

















def cql_query(registry_path:str, corpus_name:str, *query_words:str , query_break:str = None , left_word:int = None , right_word:int = None , p_tag:list = "word", s_tag:list = ["text_id"]):
    
    corpus = Corpus(registry_path = registry_path, corpus_name = corpus_name)
    corpus

    
    li = []
        
    for i in query_words:
        li.append(i)
        #print(li)    
    
        comma_separated_strings = ','.join(li)
        #print(comma_separated_strings)

        no_commas = comma_separated_strings.replace(",", "")
        #print(no_commas)
            
            
            
        result_count = corpus.query(f"{no_commas}",context_break = query_break, context_left = left_word, context_right = right_word)
    
    #result_count = corpus.query(f'"{query}"')
    store = f"Total number of query word in this corpus are {result_count}."

    #########################################################################

    with open(f'{corpus_name}_with_search_{query_words}.txt', 'w') as f:
        print(store, file=f)

    read_file = pd.read_csv (f'{corpus_name}_with_search_{query_words}.txt', on_bad_lines='skip')
    read_file.to_csv (f'{corpus_name}_with_search_{query_words}.csv', index=None)


#################################################################################################################


#################################################################################################################

    col_names = ["col1"]
    df_read = read_file.to_csv (f'{corpus_name}_with_search_{query_words}.csv',index=None)
    df1 = pd.read_csv(f"{corpus_name}_with_search_{query_words}.csv", names=col_names)
    store_csv = df1.to_csv (f'{corpus_name}_with_search_{query_words}.csv', index=None)
    df1

    df2 = pd.DataFrame(df1)

    res = ''.join(df2['col1'])

            #res
    with open(f'{corpus_name}_single_line_out_word_{query_words}.txt', 'w') as f:
        print(res, file=f)  

    read_file = pd.read_csv (f'{corpus_name}_single_line_out_word_{query_words}.txt', on_bad_lines='skip')
    read_file.to_csv (f'{corpus_name}_single_line_out_word_{query_words}.csv', index=None)    

    col_names = ["col1"]
    #df_read = read_file.to_csv (r'single_line_out.csv',index=None)
    df3 = pd.read_csv(f"{corpus_name}_single_line_out_word_{query_words}.csv", names=col_names)
    store_csv = df3.to_csv (f'{corpus_name}_out_csv_single_word_{query_words}.csv', index=None)
    df3

    read_csv = pd.read_csv(f"{corpus_name}_out_csv_single_word_{query_words}.csv")
    read_csv

    new_file_splite = read_csv.col1.str.split(expand=True)
    store_csv_final = new_file_splite.to_csv (f'{corpus_name}_single_word_word_{query_words}.csv', index=None)
    store_csv_final

    read_csv_single = pd.read_csv(f"{corpus_name}_single_word_word_{query_words}.csv")
    read_csv_single

    #df_new = read_csv_single.filter(['0','1','2','3','4','5','6','7','8','9','10','11','12','13', '14','15'])
    df_new = read_csv_single.filter(['15'])

    df_new

    store_csv_single = df_new.to_csv (f'{corpus_name}_plotting_word_{query_words}.csv', header=False , index=False)
    store_csv_single

    data= pd.read_csv(f"{corpus_name}_plotting_word_{query_words}.csv")
    data

    #########################################################################

    headerList = ["Corpus"]
    with open(f"{corpus_name}_plot_word_{query_words}.csv", 'a') as file:
        dw = csv.DictWriter(file, delimiter=',', fieldnames=headerList)
        dw.writeheader()
        
    with open(f'{corpus_name}_plot_word_{query_words}.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow(data)

    data = pd.read_csv(f'{corpus_name}_plot_word_{query_words}.csv')
    df = pd.DataFrame(data)

    df = pd.read_csv(f'{corpus_name}_plot_word_{query_words}.csv')

    #########################################################################

    li = []
        
    for i in query_words:
        li.append(i)
        #print(li)    
    
        comma_separated_strings = ','.join(li)
        #print(comma_separated_strings)

        no_commas = comma_separated_strings.replace(",", "")
        #print(no_commas)
            
            
            
        result_count = corpus.query(f"{no_commas}",context_break = query_break, context_left = left_word, context_right = right_word)
        result_df = result_count.concordance( p_show = p_tag , s_show = s_tag , form = 'kwic', cut_off = None)
    
    
    #final = result_count.concordance(form='kwic')
    #final
    #final.to_csv("data_frame_query.csv", index = None)
    result_df.to_csv(f"{corpus_name}_data_frame_query_{query_words}.csv")

    #########################################################################
    num_res = pd.read_csv(f'{corpus_name}_plot_word_{query_words}.csv')
    df_res = pd.read_csv(f'{corpus_name}_data_frame_query_{query_words}.csv')

    pd.concat([num_res,df_res])

    #########################################################################

    cor = df.iloc[0, df.columns.get_loc("Corpus")]
    #th = df.iloc[0, df.columns.get_loc("Total_Hit")]

    #########################################################################

    df = pd.concat([num_res,df_res])
    #df = df.replace(np.nan, "EXAMPLE")
    df['Corpus'] = df["Corpus"].replace(np.nan, cor)
    #df['Total_Hit'] = df["Total_Hit"].replace(np.nan, th)

    #########################################################################

    df = df.iloc[1:]
    df.to_csv(f'{corpus_name}_{query_words}_output.csv', index = None)
    
    #########################################################################
    csv_files = [f for f in os.listdir() if f.endswith(f'_{query_words}.csv')]
    txt_files = [f for f in os.listdir() if f.endswith(f'_{query_words}.txt')]


    for i in csv_files:
        current_path = os.path.abspath(os.getcwd())
        file_path = f"{current_path}/{i}"
        os.remove(file_path)
        
    for j in txt_files:
        current_path = os.path.abspath(os.getcwd())
        file_path = f"{current_path}/{j}"
        os.remove(file_path)
    
    ########################################################################
    
    df_res1 = pd.read_csv(f'{corpus_name}_{query_words}_output.csv')
    return (df_res1)