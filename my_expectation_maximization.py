from dt.my_create_db_dresden import trans
from copy import deepcopy
import numpy as np
import duckdb
from datadiscoverybench.utils import load_dresden_db, load_git_tables_db
import nltk

class Tokenizer():
    def __init__(self) -> None:
        pass

    def tokenize(self, text):

        stopwords = ['a', 'the', 'of', 'on', 'in', 'an', 'and', 'is', 'at', 'are', 'as', 'be', 'but', 'by', 'for', 'it',
                     'no', 'not', 'or',

                     'such', 'that', 'their', 'there', 'these', 'to', 'was', 'with', 'they', 'will', 'v', 've',
                     'd']  # , 's']

        # punct = [',', '.', '!', ';', ':', '?', "'", '"']
        punct = ['!', '?', "'", '"']
        try:

            tok = ' '.join([w for w in nltk.tokenize.casual_tokenize(text, preserve_case=False) if w not in stopwords])
            tok = ' '.join([w for w in nltk.tokenize.wordpunct_tokenize(tok) if w not in punct])
        except:
            return None
        return tok

class Scoring_and_Ranking:

    def __init__(self):
        self.table_scores = {}
        self.answer_scores = {}
        self.alpha = 0.99
        self.epsilon = 0.001
        self.max_iter = 50

    def update_table_scores(self, tables):
        for tid, table in tables.items():
            good = 0
            bad = 0
            uncovered_X = set([k for k, v in self.answer_scores.items() if len(v) > 0])
            for inp, out in table:
                uncovered_X.remove(inp)
                score = self.answer_scores[inp].get(out, 0)
                if score == max(self.answer_scores[inp].values()): good += score
                else: bad += 1
            unseen_X = sum([max(self.answer_scores[inp].values()) for inp in uncovered_X])
            prior = 0.5
            new_score = (prior * good) / (prior * good + (1 - prior) * (bad + unseen_X))
            self.table_scores[tid] = self.alpha * new_score
        # print("self.table_scores:", self.table_scores)

    def update_answer_scores(self, tables, queries):
        for xq in queries:
            score_None = 1
            for tid, table in tables.items():
                table_score = self.table_scores[tid]
                table_answer = None
                for x, y in table:
                    if x == xq: table_answer = y
                if table_answer is None: continue
                score_None *= 1 - table_score
                for yq, score in self.answer_scores[xq].items():
                    if score == 0:
                        score_new = 1
                    else:
                        score_new = score
                    if table_answer == yq:
                        self.answer_scores[xq][yq] = score_new * table_score
                    else:
                        self.answer_scores[xq][yq] = score_new * (1 - table_score)
            score_sum = sum(self.answer_scores[xq].values()) + score_None
            for yq in self.answer_scores[xq]:
                self.answer_scores[xq][yq] /= score_sum

    def Expectation_Maximization(self, examples, queries, rw):
        queries = set(n for n in queries)
        xe = tuple(item[0] for item in examples)
        ye = tuple(item[1] for item in examples)
        self.answer_scores = {inp: {out: 1} for inp, out in examples}
        self.answer_scores.update({q: {} for q in list(queries)})
        iter_num = 0
        inp_vals = set(map(lambda ex: ex[0], examples)) | queries
        finishedQuerying = False
        answers_old = deepcopy(self.answer_scores)

        Trans = trans()
        while True:
            if not finishedQuerying:
                finishedQuerying = True
                print("iter:", iter_num)
                answers = []
                for inp, outs in self.answer_scores.items():
                    if not outs: continue
                    vals, scores = zip(*outs.items())
                    idx = np.argmax(scores)
                    if (1 - sum(scores)) < scores[idx]:
                        answers.append((inp, vals[idx]))
                queries = queries - set(map(lambda ex: ex[0], answers))
                print("every_examples:", answers)
                found_tables = Trans.direct_transformation(answers, queries)

                tables = {}
                for tableId, pair in found_tables.items():
                    table = []
                    for inp, out in pair:
                        # if inp in list(queries) or (inp in xe and out in ye):
                        if inp in inp_vals:
                            table.append((inp, out))
                            if inp in queries and out not in self.answer_scores[inp]:
                                self.answer_scores[inp][out] = 0
                                finishedQuerying = False
                    tables[tableId] = table

            self.update_table_scores(tables)
            self.update_answer_scores(tables, queries)
            score_diff = 0
            for x, yy in self.answer_scores.items():
                for y, score in yy.items():
                    score_old = answers_old.get(x, {}).get(y, 0)
                    score_diff += abs(score - score_old)
            rw.em_scores(iter_num, self.answer_scores, self.table_scores, score_diff)
            if (finishedQuerying and score_diff < self.epsilon) or iter_num > self.max_iter:
                print("tables:", len(list(self.table_scores.keys())))
                # print("self.table_scores:", self.table_scores)
                # print("self.answer_scores:", self.answer_scores)
                print("------------------------")
                break
            answers_old = deepcopy(self.answer_scores)
            iter_num += 1
            print("------------------------")

        return self.answer_scores
