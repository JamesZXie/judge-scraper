import json
import glob

NI_PATH = "../n_il/json/*.json"
NG_PATH = "../n_ga/json/*.json"

# getCaseKeys(path) -> check keys for all json files to check for outliers
# path - for files (either Illinois or Georgia Data)

# output for NI_PATH: {
# u'terminating_date': 23169, 
# u'defendants': 23002,
# u'case_name': 23169,
# u'filing_date': 23169,
# u'case_type': 23172,
# u'case_id': 23172,
# u'year': 23172,
# u'judge': 23169,
# u'plaintiffs': 23001,
# u'docket': 23172,
# u'in_regards': 169 }

# output for NG_PATH: {
# u'terminating_date': 11397,
# u'defendants': 11397,
# u'case_name': 11397,
# u'filing_date': 11397,
# u'case_type': 11397,
# u'case_id': 11397,
# u'year': 11397,
# u'judge': 11397,
# u'plaintiffs': 11397,
# u'docket': 11397}

def getCaseKeys(path):
    keys = {}
    for x in glob.glob(path):
        with open(x) as f:
            data = json.load(f)
            for k in data.keys():
                if keys.get(k) == None:
                    keys[k] = 0
                else:
                    keys[k] += 1
    return keys


# # docket- drop number column, it's ordered by date so that shouldn't matter
# print(data["docket"][0][1])
print(getCaseKeys(NG_PATH))