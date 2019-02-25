import json
import glob
import csv
from difflib import SequenceMatcher

FED_JUDGES_PATH = "../judges/judges.csv"

NI_PATH = "../n_il/json/*.json"
NG_PATH = "../n_ga/json/*.json"

NIJudgeStringsToIgnore = ['Unassigned', 'Miscellaneous', 'Executive', 'Material', 'Claimant', 'Magistrate', 'Intervenor', 'USA', 'Respondent', 'Movant', 'Prisoner', 'Fugitive', '$15,000,000']
NIJudgeStringsToDrop = ['Member', 'Lead', 'Cause:', 'Referred', 'Demand:', 'related\xc2']

NIJudgeStringsToIgnoreV2 = ['Executive', 'Committee', 'Unassigned', 'Magistrate', 'General', 'Fugitive', 'Calendar']




# def listdiff(a, b):


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

IL_JUDGES_IDS = {'David H. Coar': 1379261, 'Edmond E. Chang': 1393546, 'Sidney I. Schenkier': None, 'Robert M. Dow, Jr': 1392656, 'Daniel G. Martin': None, 'Andrea R. Wood': 1394291, 'Arlander Keys': None, 'William J. Hibbler': 1390936, 'Suzanne B. Conlon': 1379411, 'Martin C. Ashman': None, 'Thomas M. Durkin': 1394096, 'Frederick J. Kapala': 1392566, 'James B. Zagel': 1390251, 'Iain D. Johnston': None, 'Joan B. Gottschall': 1381411, 'Blanche M. Manning': 1384276, 'Charles P. Kocoras': 1383441, 'Milton I. Shadur': 1387651, 'Jeffrey Cole': None, 'Sheila M. Finnegan': None, 'Mary M. Rowland': None, 'Sara L. Ellis': 1394286, 'Nan R. Nolan': None, 'William T. Hart': 1381881, 'John Robert Blakey': 1394726, 'Jeffrey T. Gilbert': None, 'Morton Denlow': None, 'Maria Valdez': None, 'John Z. Lee': 1393976, 'Susan E. Cox': None, 'Wayne R. Andersen': 1377171, 'Ronald A. Guzman': 1391041, 'Ruben Castillo': 1378946, 'John W. Darrah': 1391241, 'Gary Feinerman': 1393271, 'M. David Weisman': None, 'Rebecca R. Pallmeyer': 1390846, 'Sharon Johnson Coleman': 1393276, 'John F. Grady': 1381426, 'Harry D. Leinenweber': 1383791, 'Elaine E. Bucklo':
1378516, 'James F. Holderman': 1382291, 'Marvin E. Aspen': 1377301, 'Michael M. Mihm': 1385101, 'Richard A. Posner': 1386511, 'George M. Marovich': 1384321, 'Amy J. St. Eve': 1391621, 'Philip G. Reinhard': 1386836, 'Robert W. Gettleman': 1381141, 'Samuel Der-Yeghiayan': 1391901, 'Geraldine Soat Brown': None, 'Young B. Kim': None, 'Matthew F. Kennelly': 1390941, 'Joan H. Lefkow': 1391246, 'Charles R. Norgle, Sr': 1385811, 'P. Michael Mahoney': None, 'John J. Tharp, Jr': 1393986, 'George W. Lindberg': 1383916, 'John A. Nordberg': 1385801, 'Jorge L. Alonso': 1394711, 'Manish S. Shah': 1394466, 'Virginia M. Kendall': 1392326, 'Michael T. Mason': None}

GA_JUDGES_IDS = {'Willis B. Hunt, Jr': 1382566, 'J. Owen Forrester': 1380856, 'Thomas W. Thrash, Jr': 1390576, 'Julie E. Carnes': 1378851, 'Charles A. Pannell, Jr': 1391056, 'Richard W. Story': 1390581, 'Timothy C. Batten, Sr': 1392346, 'Judge Alan J. Baverman': None, 'Jack T. Camp': 1378766, 'William S. Duffey, Jr': 1392146, 'Marvin H. Shoob': 1387791, 'Orinda D. Evans': 1380566, 'Clarence Cooper': 1379481, 'Mark H. Cohen': 1394651, 'Steve C. Jones': None, 'Judge Janet F. King': None, 'Eleanor L. Ross': 1394656, 'Judge Linda T. Walker': None, 'Judge E. Clayton Scofield, III': None, 'Amy Totenberg': 1393621, 'Beverly B Martin': 1391216, 'Robert L Hinkle': 1382196, 'Judge Russell G. Vineyard': None, 'Judge Gerrilyn G. Brill': None, 'Leigh Martin May': 1394641, 'Michael L. Brown': 4028216, 'Judge C. Christopher Hagy': None, 'Richard W Goldberg': 1392856, 'Harold L. Murphy': 1385531, 'Thomas F Hogan': 1382271, 'Robert L. Vining, Jr': 1389176, 'Charles A. Moye, Jr': 1385451, 'G. Ernest Tidwell': 1388806, 'Judge Susan S. Cole': None, 'Horace T. Ward': 1389361, 'Judge Justin S. Anand': None, 'William M Ray, II': 5373556, 'Judge Walter E. Johnson': None, 'Judge Catherine M. Salinas': None, 'Judge John K. Larkins, III': None, 'Judge J. Clay Fuller': None, 'M.G. Lynn': None}

def getCaseKeys(path):
    keys = {}
    for x in glob.glob(path):
        with open(x) as f:
            data = json.load(f)
            for k in data.keys():
                if keys.get(k) == None:
                    keys[k] = 1
                else:
                    keys[k] += 1
    return keys

def getIlJudgeNames(path, judgeStringsToIgnore, judgeStringsToDrop):
    judges = {}
    for x in glob.glob(path):
        with open(x) as f:
            data = json.load(f)
            # skip if there's no judge field
            if(data.get("judge") == None):
                continue

            j = data["judge"].split()[0:3]

            # skip if there's no judge assigned
            if len(set(judgeStringsToIgnore) & set(j)) > 0:
                continue

            #drop extraneous strings
            #TODO - handle escape character
            j = list(set(j).difference(set(judgeStringsToDrop)))

            j = ' '.join(j)

            # Judge Amy J. St. Eve has four strings, so we append eve if current iteration is her
            if 'Amy' in j:
                j = j + ' Eve'

            if judges.get(j) == None:
                judges[j] = 1
            else:
                judges[j] += 1
    return judges

# refined for new json files

def getIlJudgeNamesv2(path, judgeStringsToIgnore):
    judges = {}
    for x in glob.glob(path):
        with open(x) as f:
            data = json.load(f)

            if(data.get("judge") == None):
                continue

            j = data["judge"].split()

            if len(set(judgeStringsToIgnore) & set(j)) > 0:
                continue

            j.pop(0)
            j = ' '.join(j)

            if judges.get(j) == None:
                judges[j] = 1
            else:
                judges[j] += 1
    return judges

def getFederalJudgesWithIds(path):
    judgesWithIDs = {}
    with open(path) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            name = ""
            if row[3]:
                name += row[3].strip()
            if row[2]:
                name += ' ' + row[2].strip()[0] + '.'
            if row[4]:
                name += ' ' + row[4].strip()
            if row[5]:
                name += ' ' + row[5].strip()
            judgesWithIDs[name.strip()] = row[0].strip()
    return judgesWithIDs

def matchIlCaseIdsToJudgeIds(path, judgeStringsToIgnore, judgesWithId):
    mapping = {}
    for x in glob.glob(path):
        with open(x) as f:
            data = json.load(f)

            if(data.get("judge") == None):
                continue

            j = data["judge"].split()

            if len(set(judgeStringsToIgnore) & set(j)) > 0:
                continue

            j.pop(0)
            j = ' '.join(j)

            mapping[data["case_id"]] = judgesWithId[j]
    with open('../output/il_case_to_judge_id.txt', 'w') as file:
     file.write(json.dumps(mapping))
    return mapping

def getIlCivilPartyNames(path):
    parties = {}
    for x in glob.glob(path):
        with open(x) as f:
            data = json.load(f)

            if(data.get("defendants") == None):
                continue
            if(data["case_type"] == "cr"):
                continue

            # if parties.get(j) == None:
            #     parties[j] = 1
            # else:
            #     parties[j] += 1
            
            # if(data.get("plaintiffs") == None):
            #     continue
            d = [*data["defendants"]]
            print(d)
            # break
            
    return parties

# # docket- drop number column, it's ordered by date so that shouldn't matter
# print(data["docket"][0][1])
# print(SequenceMatcher(None, 'Virginia Montana Kendall', 'Virginia M. Kendall').ratio())

# print(getIlJudgeNames(NI_PATH, NIJudgeStringsToIgnore, NIJudgeStringsToDrop))
# print(getIlJudgeNamesv2(NI_PATH_V2, NIJudgeStringsToIgnoreV2))
# print(getFederalJudgesWithIds(FED_JUDGES_PATH))
# print(matchCaseIdsToJudgeIds(NI_PATH_V2, NIJudgeStringsToIgnoreV2, IL_JUDGES_IDS))

# print(getIlCivilPartyNames(NI_PATH_V2))


def count_cases(path):
    count = 0
    for x in glob.glob(path):
        with open(x) as f:
            data = json.load(f)
            if data.get("case_id") == None:
                continue
            count += 1
    return count

def date_breakdown(path):
    filing_dates = {}
    for x in glob.glob(path):
        with open(x) as f:
            data = json.load(f)
            curr_filing_date = ''
            try:
                curr_filing_date = data["filing_date"].split('/')
                curr_filing_date = curr_filing_date[0] + '/' + curr_filing_date[2]
            except:
                pass
            if filing_dates.get(curr_filing_date) == None:
                filing_dates[curr_filing_date] = 1
            else:
                filing_dates[curr_filing_date] += 1
    return filing_dates

# print(date_breakdown(NI_PATH_V2))

def getGaJudgeNames(path):
    judges = {}
    for x in glob.glob(path):
        with open(x) as f:
            data = json.load(f)

            if(data.get("judge") == None):
                continue

            j = data["judge"].split()

            # if len(set(judgeStringsToIgnore) & set(j)) > 0:
            #     continue

            j.pop(0)
            j = ' '.join(j)

            if judges.get(j) == None:
                judges[j] = 1
            else:
                judges[j] += 1
    return judges

def matchGaCaseIdsToJudgeIds(path, judgeStringsToIgnore, judgesWithId):
    mapping = {}
    for x in glob.glob(path):
        with open(x) as f:
            data = json.load(f)

            if(data.get("judge") == None):
                continue

            j = data["judge"].split()

            if len(set(judgeStringsToIgnore) & set(j)) > 0:
                continue

            j.pop(0)
            j = ' '.join(j)

            mapping[data["case_id"]] = judgesWithId[j]
    with open('../output/ga_case_to_judge_id.txt', 'w') as file:
     file.write(json.dumps(mapping))
    return mapping

matchGaCaseIdsToJudgeIds(NG_PATH, ['Unassigned'], GA_JUDGES_IDS)
