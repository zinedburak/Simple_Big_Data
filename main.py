import sqlite3
import pandas as pd
import xlsxwriter
import matplotlib.pyplot as plt;
import multiprocessing as mp
from time import time
import numpy as np

con = sqlite3.connect("burak_netas.db")
cursor = con.cursor()

results = []


def reformat(tuple):
    tuple = str(tuple)
    tuple = tuple.replace("(", "")
    tuple = tuple.replace(",", "")
    tuple = tuple.replace(")", "")
    tuple = tuple.replace("'", "")
    return tuple


def reduce_graph(dictianory):
    tmp = dict()
    for keys, values in dictianory.items():
        if values > 1000:
            tmp[keys] = values

    return tmp


def general_show(query):
    cursor.execute(query)
    result = cursor.fetchall()

    for i in range(len(result)):
        print(result[i])


def show_all():
    print("Table")
    query = "select * from netas_data"
    general_show(query)
    print("End the Table")


def protocol_count():
    protocol_dic = dict()
    cursor.execute("select Protocol from netas_data")
    result = cursor.fetchall()
    for protocol in result:
        protocol = reformat(protocol)
        if protocol in protocol_dic.keys():
            protocol_dic[protocol] += 1
        else:
            protocol_dic[protocol] = 1
    return reduce_graph(protocol_dic)


def source_count():
    source_dic = dict()
    cursor.execute("select Source from netas_data")
    result = cursor.fetchall()
    for source in result:
        source = reformat(source)
        if source in source_dic.keys():
            source_dic[source] += 1
        else:
            source_dic[source] = 1
    return reduce_graph(source_dic)


def destination_count():
    destination_dic = dict()
    cursor.execute("select Destination from netas_data")
    result = cursor.fetchall()
    for destination in result:
        destination = reformat(destination)
        if destination in destination_dic.keys():
            destination_dic[destination] += 1
        else:
            destination_dic[destination] = 1
    return reduce_graph(destination_dic)


def method(query):
    general_dictionary = dict()
    cursor.execute(query)
    result = cursor.fetchall()
    for data in result:
        data = reformat(data)
        if data in general_dictionary:
            general_dictionary[data] += 1
        else:
            general_dictionary[data] = 1

    general_dictionary = reduce_graph(general_dictionary)
    name = query.split(" ")
    creating_table(general_dictionary, name[1], 10)


def length_count():
    length_dic = dict()
    cursor.execute("select Length from netas_data")
    result = cursor.fetchall()
    for length in result:
        length = reformat(length)
        if length in length_dic.keys():
            length_dic[length] += 1
        else:
            length_dic[length] = 1
    return reduce_graph(length_dic)


def info_count():
    info_dic = dict()
    cursor.execute("select Info from netas_data")
    result = cursor.fetchall()
    for info in result:
        info = reformat(info)
        if info in info_dic.keys():
            info_dic[info] += 1
        else:
            info_dic[info] = 1
    return reduce_graph(info_dic)


def creating_table(dictionary, name, number):
    # matplotla çalışan kod

    keys = dictionary.keys()
    y_pos = np.arange(len(keys))
    values = dictionary.values()

    plt.bar(y_pos, values, align='center')
    plt.xticks(y_pos, keys)
    plt.title(name)
    plt.show()


def collect_results(result):
    global results
    results.append(result)


"""

    workbook = xlsxwriter.Workbook('grafikler.xlsx')
    worksheet = workbook.add_worksheet()
    bold = workbook.add_format({'bold': 1})

    headings = dictionary.keys()
    values = dictionary.values()
    worksheet.write_column('A1', headings, bold)
    worksheet.write_column('B1', values)

    chart1 = workbook.add_chart({'type': 'column'})
    chart1.add_series({
        'values': '=Sheet1!$B$1:$B${}'.format(number, len(values)),
        'categories': '=Sheet1!$A$1:$B${}'.format(number, len(values)),
        'gap': 50,
    })

    worksheet.insert_chart('D2', chart1)
    workbook.close()



    keys = dictionary.keys()
    values = dictionary.values()

    df = pd.DataFrame(values)
    excel_file = 'column.xlsx'
    sheet_name = 'Sheet1'

    writer = pd.ExcelWriter(excel_file,engine='xlsxwriter')
    df.to_excel(writer,sheet_name=sheet_name)

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    chart = workbook.add_chart({'type':'column'})
    chart.add_series({
        'values': '=Sheet1!$B$2:$B${}'.format(len(values)+1),
        'gap': 2,
    })
    worksheet.insert_chart('D2', chart)
    writer.save()
"""
"""""
start = time()
creating_table(info_count(), "info", 1)
creating_table(length_count(), "length", 2)
creating_table(protocol_count(), "protocol", 3)
creating_table(destination_count(), "destination", 4)
creating_table(source_count(), "source", 5)
end = time()
print(end-start)
"""
if __name__ == '__main__':
    queries = ["select Info from netas_data", "select Length from netas_data", "select Destination from netas_data",
               "select Source from netas_data", "select Protocol from netas_data"]
    pool = mp.Pool()
    pool.map(method, queries)
    pool.close()

    """""
    start = time()
    creating_table(info_count(), "info", 1)
    creating_table(length_count(), "length", 2)
    creating_table(protocol_count(), "protocol", 3)
    creating_table(destination_count(), "destination", 4)
    creating_table(source_count(), "source", 5)
    end = time()
    print(end - start)
"""
