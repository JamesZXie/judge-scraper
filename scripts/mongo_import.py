import json
import glob
import csv
from pymongo import MongoClient

NI_V2_PATH = "../n_il/json_v2/*.json"
FED_JUDGES_PATH = "../judges/judges.csv"
CASE_JUDGES_PATH = "../output/il_case_to_judge_id.txt"

client = MongoClient('localhost', 27017)

db = client['crxss_db']
collection_cases = db['cases']
collection_judges = db['judges']

def sanitize_keys(d):
    new = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = sanitize_keys(v)
        new[k.replace('.', '')] = v
    return new

def import_cases(cases_path, case_judges_path, district):
    with open(case_judges_path) as f:
        case_judges = json.load(f)
        for x in glob.glob(cases_path):
                with open(x) as f:
                    assigned_judge = None
                    data = json.load(f)

                    if data.get("case_id") == None:
                        continue

                    if case_judges.get(data["case_id"]) != None:
                        assigned_judge = case_judges[data["case_id"]]

                    data["judge"] = [data["judge"], assigned_judge]
                    data["_id"] = data.pop("case_id")
                    data["district"] = district

                    data = sanitize_keys(data)
                    collection_cases.insert(data)

def import_judges(path):
    judge_template = {}
    with open(path) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for i, row in enumerate(readCSV):
            if i == 0:
                for field in row:
                    judge_template[field] = None
                continue
            
            curr_judge = dict(judge_template)

            for j, key in enumerate(curr_judge):
                curr_judge[key] = row[j]

            curr_judge['_id'] = curr_judge.pop('nid')
            collection_judges.insert_one(curr_judge)
            

def main():
    db_list = client.list_database_names()
    if "crxss_db" in db_list:
        print("The database exists. Please drop before re-importing.")
        return

    import_cases(NI_V2_PATH, CASE_JUDGES_PATH, "Northern District of Illinois")
    import_judges(FED_JUDGES_PATH)

main()