from dt.my_expectation_maximization import Scoring_and_Ranking
from os.path import basename, splitext
from pandas import read_csv
import pandas as pd
import numpy as np
import time
from dt.reporter import Reporter
from dt.my_create_db_dresden import trans
import nltk

examples_num = 5
random_num = 5


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

def read_csv(filename):
    """
    function: Read the contents of a csv or txt document into a dataframe
    :param: filename, given the absolute path of the benchmark file
    :return: benchmark in dataframe
    """
    # filename = "/home/wang/remote/DataDiscoveryBenchmark/benchmark/CountryToLanguage.csv"
    try:
        txt = np.loadtxt(filename)
        benchmark = pd.DataFrame(txt)
    except:
        benchmark = pd.read_csv(filename, encoding="iso-8859-1", engine="python", encoding_errors="strict").astype(str)

    return benchmark

def process_benchmark(benchmark, examples_num):
    """
    function: split the benchmark to the list of examples and queris respectively
    :param benchmark -> Dataframe: benchmark in dataframe
    :param examples_num -> int: Randomly select examples_num pairs from the benchmark
    :return -> list: Separated x and y in examples and queris from benchmark
    """
    if examples_num > len(benchmark):
        raise ValueError("examples must more than benchmark")
    examples = benchmark.sample(n=random_num, random_state=None)
    queries = benchmark.drop(examples.index)
    x_e = examples.iloc[:, 0].values.tolist()
    y_e = examples.iloc[:, -1].values.tolist()
    x_q = queries.iloc[:, 0].values.tolist()
    y_q = queries.iloc[:, -1].values.tolist()
    # if examples_num > len(benchmark):
    #     raise ValueError("")
    # x_col = benchmark.iloc[:, 0].values.tolist()
    # y_col = benchmark.iloc[:, -1].values.tolist()
    #
    # x_col = data.iloc[:, 0].values.tolist()
    # y_col = data.iloc[:, -1].values.tolist()
    # x_e, x_q = x_col[:examples_num], x_col[examples_num:]
    # y_e, y_q = y_col[:examples_num], y_col[examples_num:]

    return x_e, x_q, y_e, y_q

def pre_processing(res):
    """
    function: Remove the spaces before and after each string, lowercase strings
    :return: examples, queries, groundtruth
    """
    examples = list(zip(res[0], res[2]))
    queries = res[1]
    groundtruth = list(zip(res[1], res[3]))

    # examples = [(str(item[0].strip()), str(item[1].strip())) for item in examples]
    # queries = {str(item.strip()) for item in queries}
    # groundtruth = [(str(item[0].strip()), str(item[1].strip())) for item in groundtruth]

    examples = {(str(item[0].lower()), str(item[1].lower())) for item in examples}
    queries = {str(item.lower()) for item in queries}
    groundtruth = [(str(item[0].lower()), str(item[1].lower())) for item in groundtruth]

    Toker = Tokenizer()
    examples = {(str(Toker.tokenize(item[0])), str(Toker.tokenize(item[1]))) for item in examples}
    queries = {str(Toker.tokenize(item)) for item in queries}
    groundtruth = [(str(Toker.tokenize(item[0])), str(Toker.tokenize(item[1]))) for item in groundtruth]

    return examples, queries, groundtruth

def multi_trans(filepath):
    Trans = trans()
    start = time.time()
    filename = splitext(basename(filepath))[0]
    benchmark = read_csv(filepath)
    if examples_num > len(benchmark):
        raise ValueError("examples must more than benchmark")
    examples = benchmark.sample(n=random_num, random_state=None)
    queries = benchmark.drop(examples.index)
    x_e_1 = examples.iloc[:, 0].values.tolist()
    x_e_2 = examples.iloc[:, 1].values.tolist()
    y_e = examples.iloc[:, -1].values.tolist()
    x_q_1 = queries.iloc[:, 0].values.tolist()
    x_q_2 = queries.iloc[:, 0].values.tolist()
    y_q = queries.iloc[:, -1].values.tolist()

    examples = set(zip(zip(x_e_1, x_e_2), y_e))
    queries = set(zip(x_e_1, x_e_2))
    groundtruth = set(zip(zip(x_q_1, x_q_2), y_q))

    examples = [((str(item[0][0].strip()), str(item[0][1].strip())), str(item[1].strip())) for item in examples]
    queries = {(str(item[0].strip()), str(item[1].strip())) for item in queries}
    # groundtruth = [(str(item[0].strip()), str(item[1].strip())) for item in groundtruth]

    examples = {((str(item[0][0].lower()), (str(item[0][1].lower()))), str(item[1].lower())) for item in examples}
    queries = {(str(item[0].lower()), str(item[1].lower())) for item in queries}
    # groundtruth = [(str(item[0].lower()), str(item[1].lower())) for item in groundtruth]

    Toker = Tokenizer()
    examples = {((str(Toker.tokenize(item[0][0])), str(Toker.tokenize(item[0][1]))), str(Toker.tokenize(item[1]))) for item in examples}
    queries = {(str(Toker.tokenize(item[0])), str(Toker.tokenize(item[1]))) for item in queries}
    # groundtruth = [(str(Toker.tokenize(item[0])), str(Toker.tokenize(item[1]))) for item in groundtruth]

    reporter = Reporter(filename)
    print("examples:", examples)
    found_tables = Trans.multi_transformation(examples, queries)
    end = time.time()
    total_time = (end - start) / 60
    print('Running total time: %s Minutes' % total_time)

    reporter.result_writer(found_tables, queries, groundtruth, examples, total_time)
    print("final_answers:", found_tables)


def main(filepath):
    start = time.time()
    filename = splitext(basename(filepath))[0]
    benchmark = read_csv(filepath)
    res = process_benchmark(benchmark, examples_num)

    examples, queries, groundtruth = pre_processing(res)
    reporter = Reporter(filename)
    scorer_obj = Scoring_and_Ranking()
    answers = scorer_obj.Expectation_Maximization(examples, queries, reporter)
    end = time.time()
    total_time = (end - start) / 60
    print('Running total time: %s Minutes' % total_time)

    reporter.result_writer(answers, queries, groundtruth, examples, total_time)
    # reporter.write_answer(answers, groundtruth, examples)
    # print("++++++++++++++++++++++")
    print("final_answers:", answers)


if __name__ == "__main__":
    # airportToCountry CityToCountry CountryToLanguage CountryToThreeLettersISOCode ElementToSymbol
    # CountryToTwoLettersISOCode MountainsOver7k2feet MountainsOver7k2meters SoccerPlayer2club SoccerPlayer2NationalTeam
    # StateToAbbrv AsciiToUnicode AirportcodeToCity CountryCodeToCountry CountryToCurrencies TimeZoneToAbbrv CountriesToContinents
    # BankToSwiftCode CountryCodeToCountry
    # bankCityBranchToSwiftCode11 fahrenheitToCelcius
    # videogameReleaseDate2Developer HeadstateCountry NovelYear2Author
    file = "/home/wang/remote/DataDiscoveryBenchmark/benchmark/CityToCountry.csv"
    main(file)
    # directory = "/home/wang/remote/DataDiscoveryBenchmark/benchmark3"
    # file_paths = []
    # for root, directories, files in os.walk(directory):
    #     for filename in files:
    #         filepath = os.path.join(root, filename)
    #         file_paths.append(filepath)
    # # file_paths = ["/home/wang/remote/DataDiscoveryBenchmark/benchmark/MountainsOver7k2feet.csv", "/home/wang/remote/DataDiscoveryBenchmark/benchmark/CityToCountry.csv"]
    # for file in file_paths:
    #     try:
    #         main(file)
    #         print("++++++++++++++++++++++")
    #     except Exception as e:
    #         print("This file is not a direct transformation:", file)
    #         print("++++++++++++++++++++++")
    #         # print("error reason:", e)
    #         continue
    #             # filepath = "/home/wang/remote/DataDiscoveryBenchmark/benchmark/CityToCountry.csv"
    # reporter = Reporter("None")
    # reporter.average_result()