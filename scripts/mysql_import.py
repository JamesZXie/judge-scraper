#@TODO Add plaintiffs, defendants, docket items once we figure out how those will be given to us / parsed
#@TODO Parse nature of suit characters and create a separate table - do same for cause, jury demand, jurisdiction
#@TODO investigate errors from declaring FK constraints

import mysql.connector
import json
import glob
import csv
from datetime import datetime
import re
from env import *

NI_PATH = "../n_il/json/*.json"
NG_PATH = "../n_ga/json/*.json"

FED_JUDGES_DEMOGRAPHICS_PATH = "../judges/demographics.csv"
FED_JUDGES_EDUCATION_PATH = "../judges/education.csv"
FED_JUDGES_SERVICE_PATH = "../judges/federal-judicial-service.csv"
FED_JUDGES_CAREER_PATH = "../judges/professional-career.csv"

IL_CASE_JUDGES_PATH = "../output/il_case_to_judge_id.txt"
GA_CASE_JUDGES_PATH = "../output/ga_case_to_judge_id.txt"

db_connection = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    passwd=DB_PW,
    database=DB_NAME
)

mycursor = db_connection.cursor()

def create_tables():
    mycursor.execute("DROP TABLE IF EXISTS cases")
    mycursor.execute("""
                        CREATE TABLE cases 
                        (id VARCHAR(255) PRIMARY KEY, 
                        filing_date DATE,
                        terminating_date DATE,
                        judge VARCHAR(255),
                        federal_judge_id INT,
                        year YEAR,
                        type VARCHAR(10),
                        nature_suit VARCHAR(255),
                        name VARCHAR(255),
                        jury_demand VARCHAR(255),
                        cause VARCHAR(255),
                        jurisdiction VARCHAR(255),
                        district VARCHAR(255))
                    """)

    mycursor.execute("DROP TABLE IF EXISTS federal_judges_demographics")
    mycursor.execute("""
                        CREATE TABLE federal_judges_demographics 
                        (id INT PRIMARY KEY, 
                        last_name VARCHAR(255),
                        first_name VARCHAR(255),
                        middle_name VARCHAR(255),
                        suffix VARCHAR(10),
                        birth_month INT,
                        birth_day INT,
                        birth_year INT,
                        birth_city VARCHAR(255),
                        birth_state VARCHAR(20),
                        death_month INT,
                        death_day INT,
                        death_year INT,
                        death_city VARCHAR(255),
                        death_state VARCHAR(20),
                        gender VARCHAR(20),
                        ethnicity VARCHAR(255))
                    """)
    
    mycursor.execute("DROP TABLE IF EXISTS federal_judges_education")
    mycursor.execute("""
                        CREATE TABLE federal_judges_education 
                        (nid INT, 
                        sequence INT,
                        school VARCHAR(255),
                        degree VARCHAR(255),
                        year INT,
                        PRIMARY KEY (nid, sequence))
                    """)

    mycursor.execute("DROP TABLE IF EXISTS federal_judges_career")
    mycursor.execute("""
                    CREATE TABLE federal_judges_career
                    (nid INT, 
                    sequence INT,
                    career TEXT,
                    PRIMARY KEY (nid, sequence))
                """)
    
    mycursor.execute("DROP TABLE IF EXISTS federal_judges_service")
    mycursor.execute("""
                    CREATE TABLE federal_judges_service
                    (nid INT, 
                    sequence INT,
                    court_type VARCHAR(255),
                    court_name VARCHAR(255),
                    appointment_title VARCHAR(255),
                    appointing_president VARCHAR(255),
                    appointing_president_party VARCHAR(255),
                    aba_rating VARCHAR(255),
                    ayes INT,
                    nays INT,
                    PRIMARY KEY (nid, sequence))
                """)

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

                    data["federal_judge_id"] = assigned_judge

                    try:
                        data["filing_date"] = datetime.strptime(data["filing_date"], '%m/%d/%Y').strftime('%Y-%m-%d')
                    except (ValueError, TypeError):
                        pass
                    
                    try:
                        data["terminating_date"] = datetime.strptime(data["terminating_date"], '%m/%d/%Y').strftime('%Y-%m-%d')
                    except (ValueError, TypeError):
                        pass

                    mycursor.execute("""
                                INSERT INTO cases
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (data["case_id"], data["filing_date"], data["terminating_date"], \
                                data["judge"], data["federal_judge_id"], int(data["year"]), data["case_type"], \
                                data["nature_suit"], data["case_name"], data["jury_demand"], data["cause"], data["jurisdiction"], district))
                    db_connection.commit()


def import_federal_judges_demographics(path):
    with open(path) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for i, row in enumerate(readCSV):
            if i == 0:
                continue
            nid = row[0].strip()
            last_name = row[2].strip() if row[2].strip() else None
            first_name = row[3].strip() if row[3].strip() else None
            middle_name = row[4].strip() if row[4].strip() else None
            suffix = row[5].strip() if row[5].strip() else None
            birth_month = row[6].strip() if row[6].strip() else None
            birth_day = row[7].strip() if row[7].strip() else None
            birth_year = re.sub("[^0-9]", "", row[8].strip()) if row[8].strip() else None #edge case for year containing non numeric chars
            birth_city = row[9].strip() if row[9].strip() else None
            birth_state = row[10].strip() if row[10].strip() else None
            death_month = row[11].strip() if row[11].strip() else None
            death_day = row[12].strip() if row[12].strip() else None
            death_year = row[13].strip() if row[13].strip() else None
            death_city = row[14].strip() if row[14].strip() else None
            death_state = row[15].strip() if row[15].strip() else None
            gender = row[16].strip() if row[16].strip() else None
            ethnicity = row[17].strip() if row[17].strip() else None

            mycursor.execute("""
                                INSERT INTO federal_judges_demographics
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (nid, last_name, first_name, middle_name, suffix, birth_month, birth_day, \
                                birth_year, birth_city, birth_state, death_month, death_day, death_year, \
                                death_city, death_state, gender, ethnicity))
            db_connection.commit()

def import_federal_judges_education(path):
    with open(path) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for i, row in enumerate(readCSV):
            if i == 0:
                continue
            nid = row[0].strip()
            sequence = row[1].strip() if row[2].strip() else None
            school = row[3].strip() if row[3].strip() else None
            degree = row[4].strip() if row[4].strip() else None
            year = row[5].strip()[-4:] if row[5].strip() else None #edge case where some fields contain ranges
            mycursor.execute("""
                                INSERT INTO federal_judges_education
                                VALUES (%s, %s, %s, %s, %s)
                            """, (nid, sequence, school, degree, year))
            db_connection.commit()

def import_federal_judges_career(path):
    with open(path) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for i, row in enumerate(readCSV):
            if i == 0:
                continue
            nid = row[0]
            sequence = row[1].strip() if row[2].strip() else None
            career = row[3].strip() if row[3].strip() else None
            mycursor.execute("""
                                INSERT INTO federal_judges_career
                                VALUES (%s, %s, %s)
                            """, (nid, sequence, career))
            db_connection.commit()

def import_federal_judges_service(path):
    with open(path) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for i, row in enumerate(readCSV):
            if i == 0:
                continue
            nid = row[0].strip()
            sequence = row[1].strip() if row[2].strip() else None
            court_type = row[3].strip() if row[3].strip() else None
            court_name = row[4].strip() if row[4].strip() else None
            appointment_title = row[5].strip() if row[5].strip() else None
            appointing_president = row[6].strip() if row[6].strip() else None
            appointing_president_party = row[7].strip() if row[7].strip() else None
            aba_rating = row[10].strip() if row[10].strip() else None
            if nid == "1394031": # edge case -- this case separates ayes/nays with '-'
                ayes = row[20].strip().split('-')[0] if row[20].strip() else None
                nays = row[20].strip().split('-')[-1] if row[20].strip() else None
            else:
                ayes = row[20].strip().split('/')[0] if row[20].strip() else None
                nays = row[20].strip().split('/')[-1] if row[20].strip() else None

            mycursor.execute("""
                                INSERT INTO federal_judges_service
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (nid, sequence, court_type, court_name, appointment_title, \
                                appointing_president, appointing_president_party, aba_rating, \
                                ayes, nays))
            db_connection.commit()


def main():
    create_tables()
    import_federal_judges_demographics(FED_JUDGES_DEMOGRAPHICS_PATH)
    import_cases(NI_PATH, IL_CASE_JUDGES_PATH, "Northern District of Illinois")
    import_cases(NG_PATH, GA_CASE_JUDGES_PATH, "Northern District of Georgia")
    import_federal_judges_education(FED_JUDGES_EDUCATION_PATH)
    import_federal_judges_career(FED_JUDGES_CAREER_PATH)
    import_federal_judges_service(FED_JUDGES_SERVICE_PATH)

main()