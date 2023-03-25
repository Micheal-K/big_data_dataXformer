import duckdb
from datadiscoverybench.utils import load_dresden_db, load_git_tables_db
from collections import defaultdict
import time
import os


tau = 2

def query_in_Column(x, y):
    result_in_Column = f"""
        SELECT colX1.TableId, colX1.ColumnId, colY.ColumnId
        FROM 
            (SELECT TableId, ColumnId
            FROM ALLTables
            WHERE CellValue IN {x}
            GROUP BY TableId, ColumnId
            HAVING COUNT(DISTINCT CellValue) >= {tau}
            ) AS colX1,

            (SELECT TableId, ColumnId
            FROM ALLTables
            WHERE CellValue IN {y}
            GROUP BY TableId, ColumnId
            HAVING COUNT(DISTINCT CellValue) >= {tau}
            ) AS colY

        WHERE colX1.TableId = colY.TableId
            AND colX1.ColumnId <> colY.ColumnId

    """

    return result_in_Column

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

# TableId = 'whole_tables_licensed_gold_standard'

my_query = f"""
                   SELECT *
                        FROM ALLTables
                        WHERE CellValue IN ('Book')
                       
                LIMIT 20

               """

# my_query_tableid = f"""
#                     SELECT *
#                         FROM ALLTables
#                         WHERE TableId == '{TableId}'
#
#                 """


def find_direct_trans(result_col, con, find_item):
    result_pairs = {}
    for row in range(result_col.shape[0]):
        TableId = result_col.iloc[row][0]
        ColumnId = result_col.iloc[row][1]
        ColumnId_2 = result_col.iloc[row][2]
        my_query = f"""
                        SELECT *
                            FROM ALLTables
                            WHERE TableId == '{TableId}'

                    """
        # con1 = duckdb.connect(database=':memory:')
        # load_git_tables_db(con1, parts=[part])
        df = con.execute(my_query).fetch_df()
        for xq in find_item:
            # print(find_item)
            # print(xq)
            if xq in df['CellValue'].values:
                res_1 = df.loc[df['CellValue'] == xq]
                # print("res_1:", res_1)
                res_1 = res_1.loc[df['ColumnId'] == ColumnId]
                for i in range(res_1.shape[0]):
                    RowId = res_1.iloc[i][3]
                    res_2 = df.loc[df['RowId'] == RowId]
                    res_2 = res_2.loc[df['ColumnId'] == ColumnId_2]
                    # print("res_2:", res_2)
                    yq = res_2.iloc[0][0]
                    # print(yq)
                    if len(result_pairs.keys()) == 0:
                        result_pairs[TableId] = [(xq, yq)]
                    keys = list(result_pairs.keys())
                    for key in keys:
                        if key == TableId:
                            result_pairs[key].append((xq, yq))
                        else:
                            result_pairs[TableId] = [(xq, yq)]

    return result_pairs

def find_direct_trans_in_row(result_row, con, find_item):
    # print(find_header(result_row, part))
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
        df = con.execute(my_query).fetch_df()
        for xq in find_item:
            # xq = str(xq.strip())

            if xq in df['CellValue'].values:
                res_1 = df.loc[df['CellValue'] == xq]
                res_1 = res_1.loc[df['RowId'] == RowId]
                for i in range(res_1.shape[0]):
                    ColumnId = res_1.iloc[i][2]
                    res_2 = df.loc[df['ColumnId'] == ColumnId]
                    res_2 = res_2.loc[df['RowId'] == RowId_2]
                    yq = res_2.iloc[0][0]
                    if len(result_pairs.keys()) == 0:
                        result_pairs[TableId] = [(xq, yq)]
                    keys = list(result_pairs.keys())
                    for key in keys:
                        # print(key)
                        if key == TableId:
                            result_pairs[key].append((xq, yq))
                        else:
                            result_pairs[TableId] = [(xq, yq)]
                # print("result_pairs_tmp:", result_pairs)

    return result_pairs

a = ['abstraction_tables_licensed',
     'allegro_con_spirito_tables_licensed', 'attrition_rate_tables_licensed', 'beats_per_minute_tables_licensed',
     'beauty_sleep_tables_licensed', 'bits_per_second_tables_licensed',
     'cardiac_output_tables_licensed',
     'cease_tables_licensed',
     'centripetal_acceleration_tables_licensed',
     'channel_capacity_tables_licensed',
     'clotting_time_tables_licensed',
     'command_processing_overhead_time_tables_licensed',
     'count_per_minute_tables_licensed',
     'crime_rate_tables_licensed',
     'data_rate_tables_licensed',
     'dead_air_tables_licensed',
     'dogwatch_tables_licensed',
     'dose_rate_tables_licensed',
     'dwarf_tables_licensed']

b = ["entr'acte_tables_licensed",
     'episcopate_tables_licensed',
     'erythrocyte_sedimentation_rate_tables_licensed',
     'escape_velocity_tables_licensed',
     'fertile_period_tables_licensed',
     'graveyard_watch_tables_licensed',
     'growth_rate_tables_licensed',
     'half_life_tables_licensed',
     'halftime_tables_licensed',
     'heterotroph_tables_licensed',
     'hypervelocity_tables_licensed',
     'id_tables_licensed',
     'in_time_tables_licensed',
     'incubation_period_tables_licensed',
     'indiction_tables_licensed',
     'inflation_rate_tables_licensed',
     'interim_tables_licensed',
     'kilohertz_tables_licensed',
     'kilometers_per_hour_tables_licensed',
     'lapse_tables_licensed',
     'last_gasp_tables_licensed',
     'latent_period_tables_licensed',
     'lead_time_tables_licensed',
     'living_thing_tables_licensed']
c = [
    'lunitidal_interval_tables_licensed',
    'meno_mosso_tables_licensed',
    'menstrual_cycle_tables_licensed',
    'metabolic_rate_tables_licensed',
    'miles_per_hour_tables_licensed',
    'multistage_tables_licensed',
    'musth_tables_licensed',
    'neonatal_mortality_tables_licensed',
    'object_tables_licensed',
    'orbit_period_tables_licensed',
    'organism_tables_licensed',
    'peacetime_tables_licensed',
    'peculiar_velocity_tables_licensed',
    'physical_entity_tables_licensed',
    'processing_time_tables_licensed',
    'question_time_tables_licensed',
    'quick_time_tables_licensed',
    'radial_pulse_tables_licensed',
    'radial_velocity_tables_licensed',
    'rainy_day_tables_licensed',
    'rate_of_return_tables_licensed',
    'reaction_time_tables_licensed']
d = [
    'parent_tables_licensed',
    'real_time_tables_licensed',
    'relaxation_time_tables_licensed',
    'respiratory_rate_tables_licensed',
    'return_on_invested_capital_tables_licensed',
    'revolutions_per_minute_tables_licensed',
    'rotational_latency_tables_licensed',
    'running_time_tables_licensed',
    'safe_period_tables_licensed',
    'sampling_frequency_tables_licensed',
    'sampling_rate_tables_licensed',
    'secretory_phase_tables_licensed',
    'seek_time_tables_licensed',
    'shiva_tables_licensed',
    'show_time_tables_licensed',
    'solar_constant_tables_licensed',
    'speed_of_light_tables_licensed',
    'split_shift_tables_licensed',
    'steerageway_tables_licensed',
    'stopping_point_tables_licensed',
    'terminal_velocity_tables_licensed',
    'terminus_ad_quem_tables_licensed',
    'then_tables_licensed',
    'thing_tables_licensed',
    "time-out_tables_licensed",
    'time_interval_tables_licensed',
    'time_slot_tables_licensed',
    'track-to-track_seek_time_tables_licensed',
    'usance_tables_licensed',
    'wartime_tables_licensed',
    'whole_tables_licensed'
]

tryy = ['quick_time_tables_licensed', 'id_tables_licensed']

# part_ls = ([n for n in range(10)], [n for n in range(10, 20)])
# con = duckdb.connect(database=':memory:')
# load_dresden_db(con, parts=part_ls)

# start = time.time()
# part = [n for n in range(150)]
# # print("parts:", part)
# con = duckdb.connect(database=':memory:')
# # load_git_tables_db(con, parts=[part])
# load_dresden_db(con, parts=part)
# end = time.time()
# print('load DB running time: %s Minutes' % ((end - start)*0.01666667))


class trans:
    def __init__(self):
        self.con = None
        self.con1 = None
        self.con2 = None

    def connect_memory(self):
        start = time.time()
        # self.con = duckdb.connect(database='database.duckdb')
        dir_path = '/home/wang/remote/DataDiscoveryBenchmark/datadiscoverybench/data/dresden'
        if not os.path.isdir(dir_path + '/disk_db/'):
            os.mkdir(dir_path + '/disk_db/')
        self.con = duckdb.connect(database=dir_path + '/disk_db/' + 'db1.txt')
        # print(dir_path + '/disk_db/' + 'db1.txt')
        load_dresden_db(self.con, parts=list(range(20)), store_db=False)
        print(self.con.execute("SELECT count(*) FROM AllTables").fetch_df())
        # self.con1 = duckdb.connect(database=':memory:')
        # load_dresden_db(self.con1, parts=[n for n in range(30, 60)])
        # self.con2 = duckdb.connect(database=':memory:')
        # load_dresden_db(self.con2, parts=[n for n in range(60, 90)])
        # self.con3 = duckdb.connect(database=':memory:')
        # load_dresden_db(self.con3, parts=[n for n in range(150, 200)])
        # self.con4 = duckdb.connect(database=':memory:')
        # load_dresden_db(self.con4, parts=[n for n in range(200, 250)])
        # self.con5 = duckdb.connect(database=':memory:')
        # load_dresden_db(self.con5, parts=[n for n in range(250, 300)])
        # self.con6 = duckdb.connect(database=':memory:')
        # load_dresden_db(self.con6, parts=[n for n in range(300, 350)])
        end = time.time()
        print('Running time: %s Minutes' % ((end - start) * 0.01666667))


    def direct_transformation(self, iter_num, examples, queries):
        x = tuple(item[0] for item in examples)
        y = tuple(item[1] for item in examples)
        low_queries = {item for item in queries}

        # x = tuple(str(item[0].strip()) for item in examples)
        # y = tuple(str(item[1].strip()) for item in examples)
        # low_queries = {str(item.strip()) for item in queries}
        # print(x,y,low_queries)

        tables = defaultdict(set)
        # in "1, 4, 9, 15, 17, 22" can get something
        # a + b + c + d
        # part_ls = ([n for n in range(100)], [n for n in range(100, 200)], [n for n in range(200, 300)],
        #            [n for n in range(300, 400)], [n for n in range(400, 500)])
        part_ls = ([n for n in range(50)], [n for n in range(50, 60)])
        # part_ls = ([n for n in range(50)])
        # , [n for n in range(10, 20)]
        # start = time.time()
        # con = duckdb.connect(database=':memory:')
        # load_dresden_db(con, parts=[n for n in range(220, 230)])
        # con1 = duckdb.connect(database=':memory:')
        # load_dresden_db(con1, parts=[n for n in range(230, 240)])
        # con2 = duckdb.connect(database=':memory:')
        # load_dresden_db(con2, parts=[n for n in range(240, 250)])
        # end = time.time()
        # print('Running time: %s Minutes' % ((end - start) * 0.01666667))

        if iter_num == 0:
            self.connect_memory()

        for part in range(3):
            # print("parts:", part)
            # part = [1, 2, 4, 6, 9, 10]
            # start = time.time()
            # con = duckdb.connect(database=':memory:')
            # # load_git_tables_db(con, parts=[part])
            # load_dresden_db(con, parts=part)
            # end = time.time()
            # print('Running time: %s Minutes' % ((end - start)*0.01666667))
            # result_tables = con.execute(query_in_Column(x, y)).fetch_df()
            if part == 0:
                result_tables = self.con.execute(query_in_Row(x, y)).fetch_df()
                all_answers = find_direct_trans_in_row(result_tables, self.con, set(x) | low_queries)
            # elif part == 1:
            #     result_tables = self.con1.execute(query_in_Row(x, y)).fetch_df()
            #     all_answers = find_direct_trans_in_row(result_tables, self.con1, set(x) | low_queries)
            # elif part == 2:
            #     result_tables = self.con2.execute(query_in_Row(x, y)).fetch_df()
            #     all_answers = find_direct_trans_in_row(result_tables, self.con2, set(x) | low_queries)
            # elif part == 3:
            #     result_tables = self.con3.execute(query_in_Row(x, y)).fetch_df()
            #     all_answers = find_direct_trans_in_row(result_tables, self.con3, set(x) | low_queries)
            # elif part == 4:
            #     result_tables = self.con4.execute(query_in_Row(x, y)).fetch_df()
            #     all_answers = find_direct_trans_in_row(result_tables, self.con4, set(x) | low_queries)
            # elif part == 5:
            #     result_tables = self.con5.execute(query_in_Row(x, y)).fetch_df()
            #     all_answers = find_direct_trans_in_row(result_tables, self.con5, set(x) | low_queries)
            # elif part == 6:
            #     result_tables = self.con6.execute(query_in_Row(x, y)).fetch_df()
            #     all_answers = find_direct_trans_in_row(result_tables, self.con6, set(x) | low_queries)

            # result_tables = con.execute(query_in_Row(x, y)).fetch_df()
            if not result_tables.empty:
                pass
                # print("result_tables:", result_tables)
            # all_answers = find_direct_trans(result_tables, con, set(x) | low_queries)
            # all_answers = find_direct_trans_in_row(result_tables, con, set(x) | low_queries)


            # if len(all_answers) != 0:
                # print("answers:", all_answers)
            for tableid, pairs in all_answers.items():
                for pair in pairs:
                    tables[tableid].add(pair)
        # print(tables)
        # print("parts:", part)
        # print(result_tables)
        # print("(RowId_header, RowId_2_header):", find_header(result_tables, part))
        # print("---------------------------------------")
        # con.close()
        # print("everytbale:", dict(tables))
        return dict(tables)


if __name__ == "__main__":

    examples = {('Germany', 'Berlin'), ('France', 'Paris'), ('China', 'Beijing'), ('Portugal', 'Lisbon'),
                ('Bahrain', 'Manama')}
    queries = {'Morocco', 'Fiji', 'Mali', 'Panama', 'Indonesia', 'Turkey', 'Chile', 'Cape Verde',
               'Nicaragua', 'Sierra Leone', 'Guyana', 'Oman', 'Belarus'}
    tables = direct_transformation(examples, queries)
