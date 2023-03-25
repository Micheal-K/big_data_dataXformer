from os import makedirs
from os.path import abspath, isdir, join, exists
from pandas.core.frame import DataFrame
import pandas as pd
import warnings

BASEDIR = "../reports9"
warnings.filterwarnings('ignore')

class Reporter():

    def __init__(self, dirname):
        self.dirname = dirname
        self.rootdir = self.write_path(dirname)
        self.pre = []
        self.rec = []

    def write_path(self, dirname):
        basedir = abspath(BASEDIR)
        # print(basedir)
        num = 0
        while isdir(join(basedir, dirname + str(num))):
            num += 1
        rootdir = join(basedir, dirname + str(num))
        makedirs(rootdir)
        # self.rootdir = rootdir
        return rootdir

    def result_writer(self, answers, queries, groundtruth, examples, total_time):
        inp = set(item[0] for item in examples) | set(queries)
        with open(join(self.rootdir, "result.txt"), "a+") as f:
            f.write(
                f"result: ({len(answers)}/{len(inp)}):\n"
            )
            [f.write(str(item) + "\n") for item in answers.items()]

        df = DataFrame(columns=["X", "found_Y", "correct_Y", "Score"])

        try:
            for x, y_gt in set(groundtruth) | set(examples):
                all_y = answers.get(x)
                df_dict = {"X": x, "correct_Y": y_gt}

                if all_y:
                    for out, score in all_y.items():
                        df_dict.update(**{"found_Y": out, "Score": score})
                        df = df.append(df_dict, ignore_index=True)
                else:
                    df_dict.update(**{"found_Y": "", "Score": -1.0})
                    df = df.append(df_dict, ignore_index=True)

            df = df.astype({"Score": "float"})
            df = df.sort_values(["X", "Score"], ascending=[True, False])
            df = df.loc[df.groupby("X")["Score"].idxmax()]
            total = len(df)
            answered, correct = 0, 0
            for idx, (x, y, y_gt, _) in df.iterrows():
                if y != "":
                    answered += 1
                    if y == y_gt:
                        correct += 1

            precision = correct / answered if answered else 0
            recall = answered / total if total else 0
            self.pre.append(precision)
            self.rec.append(recall)
            print(f"Precision: {precision}, Recall: {recall}")
            print(self.rootdir)

            df.to_csv(join(self.rootdir, "answers.csv"), index=False)

            if exists(join(abspath(BASEDIR), "average_result.csv")):
                df2 = pd.read_csv(join(abspath(BASEDIR), 'average_result.csv'))
                df2.loc[len(df2.index)] = [self.dirname, precision, recall]
                df2.to_csv(join(abspath(BASEDIR), "average_result.csv"), index=False)

            else:
                df1 = DataFrame(columns=["filename", "precision", "recall"])
                df1.loc[len(df1.index)] = [self.dirname, precision, recall]
                df1.to_csv(join(abspath(BASEDIR), "average_result.csv"), index=False)

            with open(join(self.rootdir, "result.txt"), "a+") as f:
                f.write(f"---------------------------\n")
                f.write(f"Precision: {precision}, Recall: {recall}\n")
                f.write('Running total time: %s Minutes\n' % total_time)
        except FutureWarning:
            # print("append-FutureWarning")
            print("")

    def average_result(self):
        df = pd.read_csv(join(abspath(BASEDIR), 'average_result.csv'))
        # Calculate the mean of the 'column_name' column
        mean_precision = df['precision'].mean()
        mean_recall = df['recall'].mean()

        if exists(join(abspath(BASEDIR), "average_result.csv")):
            df.loc[len(df.index)] = ["average result:", mean_precision, mean_recall]
            df.to_csv(join(abspath(BASEDIR), "average_result.csv"), index=False)



    def em_scores(self, iteration, answer_scores, table_scores, delta):
        with open(join(self.rootdir, "em.txt"), "a+") as f:
            f.write(
                f"Iter: {iteration}\nDelta: {delta}\nAnswer_Scores ({len(answer_scores)}):\n"
            )
            [f.write(str(item) + "\n") for item in answer_scores.items()]
            f.write(f"Table_Scores ({len(table_scores)}):\n")
            [f.write(str(item) + "\n") for item in table_scores.items()]
            f.write(f"-----------------------------\n")