import duckdb
from datadiscoverybench.utils import load_dresden_db, load_git_tables_db
from collections import defaultdict
import time
import os
from typing import Dict, List, Set, Tuple
import pandas as pd

tau = 2

def query_in_Row(x, y):

    result_in_Row = f"""
            SELECT colX1.TableId, colX1.RowId, colY.RowId
            FROM 
                (SELECT TableId, RowId
                FROM ALLTables
                WHERE CellValue IN {x}
                GROUP BY TableId, RowId
                HAVING COUNT(DISTINCT CellValue) >= {tau}
                ) AS colX1,

                (SELECT TableId, RowId
                FROM ALLTables
                WHERE CellValue IN {y}
                GROUP BY TableId, RowId
                HAVING COUNT(DISTINCT CellValue) >= {tau}
                ) AS colY

            WHERE colX1.TableId = colY.TableId
                AND colX1.RowId <> colY.RowId
            
        """
    return result_in_Row


def query_in_Row_multi(x1, x2, y):
    result_in_Row = f"""
            SELECT colX1.TableId, colX1.RowId, colX2.RowId, colY.RowId
            FROM 
                (SELECT TableId, RowId
                FROM ALLTables
                WHERE CellValue IN {x1}
                GROUP BY TableId, RowId
                HAVING COUNT(DISTINCT CellValue) >= {tau}
                ) AS colX1,
                
                (SELECT TableId, RowId
                FROM ALLTables
                WHERE CellValue IN {x2}
                GROUP BY TableId, RowId
                HAVING COUNT(DISTINCT CellValue) >= {tau}
                ) AS colX2,

                (SELECT TableId, RowId
                FROM ALLTables
                WHERE CellValue IN {y}
                GROUP BY TableId, RowId
                HAVING COUNT(DISTINCT CellValue) >= {tau}
                ) AS colY

            WHERE colX1.TableId = colY.TableId
                AND colX1.RowId <> colY.RowId
                AND colX1.tableid = colX2.tableid
                AND colX1.RowId <> colX2.RowId AND colX2.RowId <> colY.RowId

        """
    return result_in_Row

import nltk
class Tokenizer():
    def __init__(self) -> None:
        pass

    def tokenize(self, text):

        stopwords = ['a', 'the', 'of', 'on', 'in', 'an', 'and', 'is', 'at', 'are', 'as', 'be', 'but', 'by', 'for', 'it',
                     'no', 'not', 'or', 'such', 'that', 'their', 'there', 'these', 'to', 'was', 'with', 'they', 'will', 'v', 've',
                     'd']
        punct = ['!', '?', "'", '"']
        try:
            tok = ' '.join([w for w in nltk.tokenize.casual_tokenize(text, preserve_case=False) if w not in stopwords])
            tok = ' '.join([w for w in nltk.tokenize.wordpunct_tokenize(tok) if w not in punct])
        except:
            return None
        return tok

class trans:
    def __init__(self):
        self.con = None
        self.con1 = None
        self.con2 = None
        self.connect_memory()
        self.cache = {}
        self.df = None
        self.tableId_old = set()

    def candidate_table(self, result_row, con):
        tableId = set()
        for row in range(result_row.shape[0]):
            tableId.add(result_row.iloc[row][0])
        tableId = tableId - self.tableId_old
        if len(tableId) > 0:
            for i in tableId:
                my_query = f"""
                                            SELECT *
                                                FROM ALLTables
                                                WHERE TableId == '{i}'

                                        """
                if self.df is None:
                    self.df = con.execute(my_query).fetch_df()
                else:
                    new_df = con.execute(my_query).fetch_df()
                    self.df = pd.concat([self.df, new_df])

    def find_direct_trans_in_row(self, result_row, con, find_item, examples):
        Toker = Tokenizer()
        x_list = list(item[0] for item in examples)
        y_list = list(item[1] for item in examples)
        result_pairs = {}
        for row in range(result_row.shape[0]):
            TableId = result_row.iloc[row][0]
            RowId = result_row.iloc[row][1]
            RowId_2 = result_row.iloc[row][2]
            my_query = f"""
                            SELECT *
                                FROM ALLTables
                                WHERE TableId == '{TableId}'

                        """
            self.df = con.execute(my_query).fetch_df()

            # Determine if x and y are in the same row. If no, continue
            tau_num = 0
            df = self.df.loc[self.df['TableId'] == TableId]
            for x, y in examples:
                if x in df['CellValue'].values and y in df['CellValue'].values:
                    # num += 1
                    x_col = set()
                    res_1 = df.loc[df['CellValue'] == x]
                    res_1 = res_1.loc[res_1['RowId'] == RowId]
                    for i in range(res_1.shape[0]):
                        ColumnId1 = res_1.iloc[i][2]
                        x_col.add(ColumnId1)
                    y_col = set()
                    res_2 = df.loc[df['CellValue'] == y]
                    res_2 = res_2.loc[res_2['RowId'] == RowId_2]
                    for i in range(res_2.shape[0]):
                        ColumnId2 = res_2.iloc[i][2]
                        y_col.add(ColumnId2)

                    tag = False
                    for element in list(x_col):
                        if element in list(y_col):
                            tag = True
                            break
                    if tag:
                        if TableId not in result_pairs.keys():
                            result_pairs[TableId] = {(x, y)}
                        else:
                            result_pairs[TableId].add((x, y))

                        tau_num += 1
            if tau_num < 2:
                if tau_num == 1:
                    del result_pairs[TableId]
                continue

            # find the quries in condidate tables
            for xq in find_item:
                if xq in df['CellValue'].values:
                    res_1 = df.loc[df['CellValue'] == xq]
                    res_1 = res_1.loc[res_1['RowId'] == RowId]
                    for i in range(res_1.shape[0]):
                        ColumnId = res_1.iloc[i][2]
                        res_2 = df.loc[df['ColumnId'] == ColumnId]
                        res_2 = res_2.loc[res_2['RowId'] == RowId_2]
                        yq = res_2.iloc[0][0]
                        if not yq.strip():
                            continue
                        yq = str(Toker.tokenize(yq))
                        if TableId not in result_pairs.keys():
                            result_pairs[TableId] = {(xq, yq)}
                        else:
                            if xq in [x for x, y in result_pairs[TableId]]:
                                continue
                            result_pairs[TableId].add((xq, yq))

            result_list = [x for x, y in result_pairs[TableId]]
            if len([x for x in result_list if x not in x_list]) == 0:
                del result_pairs[TableId]
            self.tableId_old.add(k for k in result_pairs.keys())
        return result_pairs

    def connect_memory(self):
        start = time.time()
        dir_path = '/home/wang/remote/DataDiscoveryBenchmark/datadiscoverybench/data/dresden'
        if not os.path.isdir(dir_path + '/disk_db/'):
            os.mkdir(dir_path + '/disk_db/')
        self.con = duckdb.connect(database=dir_path + '/disk_db/' + 'db1.txt')
        self.con1 = duckdb.connect(database=dir_path + '/disk_db/' + 'db2.txt')
        self.con2 = duckdb.connect(database=dir_path + '/disk_db/' + 'db3.txt')

        # print(dir_path + '/disk_db/' + 'db1.txt')
        # load_dresden_db(self.con, parts=list(range(250)), store_db=False)
        # load_dresden_db(self.con1, parts=list(range(250, 350)), store_db=False)
        # load_dresden_db(self.con2, parts=list(range(350, 500)), store_db=False)
        # print(self.con.execute("SELECT count(*) FROM AllTables").fetch_df())
        # print(self.con1.execute("SELECT count(*) FROM AllTables").fetch_df())
        # print(self.con2.execute("SELECT count(*) FROM AllTables").fetch_df())
        end = time.time()
        print('loadung run time: %s Minutes' % ((end - start) * 0.01666667))

    def direct_transformation(self, examples: Set[Tuple[str, str]], queries: Set[str]
    ) -> Dict[str, Set[Tuple[str, str]]]:
        tables = defaultdict(set)
        x = tuple(item[0] for item in examples)
        y = tuple(item[1] for item in examples)
        low_queries = {item.lower() for item in queries}

        for i in range(2):
            if i == 0:
                result_tables = self.con.execute(query_in_Row(x, y)).fetch_df()
                all_answers = self.find_direct_trans_in_row(result_tables, self.con, low_queries, examples)
            elif i == 1:
                result_tables = self.con1.execute(query_in_Row(x, y)).fetch_df()
                all_answers = self.find_direct_trans_in_row(result_tables, self.con1, low_queries, examples)
            elif i == 2:
                result_tables = self.con2.execute(query_in_Row(x, y)).fetch_df()
                all_answers = self.find_direct_trans_in_row(result_tables, self.con2, low_queries, examples)

            if not result_tables.empty:
                pass
                # print("result_tables:", result_tables)
            for tableid, pairs in all_answers.items():
                for pair in pairs:
                    tables[tableid].add(pair)
        return dict(tables)

    def find_multi_trans_in_row(self, result_row, con, find_item, examples):
        Toker = Tokenizer()
        result_pairs = {}
        for row in range(result_row.shape[0]):
            TableId = result_row.iloc[row][0]
            RowId = result_row.iloc[row][1]
            RowId_2 = result_row.iloc[row][2]
            RowId_3 = result_row.iloc[row][3]
            my_query = f"""
                            SELECT *
                                FROM ALLTables
                                WHERE TableId == '{TableId}'

                        """
            self.df = con.execute(my_query).fetch_df()
            df = self.df

            for x1, x2 in find_item:
                if x1 in df['CellValue'].values and x2 in df['CellValue'].values:
                    print("xq:", x1)
                    res_1 = df.loc[df['CellValue'] == x1]
                    res_1 = res_1.loc[res_1['RowId'] == RowId]
                    print(res_1)
                    for i in range(res_1.shape[0]):
                        ColumnId = res_1.iloc[i][2]
                        res_2 = df.loc[df['ColumnId'] == ColumnId]
                        res_2 = res_2.loc[res_2['RowId'] == RowId_3]
                        print("res_2:", res_2)
                        yq = res_2.iloc[0][0]
                        # print(xq, yq)
                        if not yq.strip():
                            continue
                        yq = str(Toker.tokenize(yq))
                        if TableId not in result_pairs.keys():
                            result_pairs[TableId] = {((x1, x2), yq)}
                        else:
                            if (x1, x2) in [x for x, y in result_pairs[TableId]]:
                                continue
                            result_pairs[TableId].add(((x1, x2), yq))
            self.tableId_old.add(k for k in result_pairs.keys())
            # print("pairs:", result_pairs[TableId])
        return result_pairs

    def multi_transformation(self, examples: Set[Tuple[Tuple[str, str], str]], queries: Set[Tuple[str, str]]
    ) -> Dict[str, Set[Tuple[Tuple[str, str], str]]]:
        tables = defaultdict(set)
        x = list(item[0] for item in examples)
        x1 = tuple(item[0] for item in x)
        x2 = tuple(item[1] for item in x)
        y = tuple(item[1] for item in examples)
        # low_queries = {item.lower() for item in queries}

        for i in range(2):
            if i == 0:
                result_tables = self.con.execute(query_in_Row_multi(x1, x2, y)).fetch_df()
                all_answers = self.find_multi_trans_in_row(result_tables, self.con, queries, examples)
            elif i == 1:
                result_tables = self.con1.execute(query_in_Row_multi(x1, x2, y)).fetch_df()
                all_answers = self.find_multi_trans_in_row(result_tables, self.con1, queries, examples)
            elif i == 2:
                result_tables = self.con2.execute(query_in_Row_multi(x1, x2, y)).fetch_df()
                all_answers = self.find_multi_trans_in_row(result_tables, self.con2, queries, examples)
            print("result_tables:", result_tables)
            if not result_tables.empty:
                pass
                # print("result_tables:", result_tables)
            for tableid, pairs in all_answers.items():
                for pair in pairs:
                    tables[tableid].add(pair)
            # print("+_+_+_+_+_+_+__")
            # print("keys:", len(list(tables[tableid)))
        return dict(tables)

