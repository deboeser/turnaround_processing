import numpy as np
import pandas as pd
from multiprocessing import Pool, Process
import pickle
from tqdm import tqdm
import os

from datetime import datetime
from datetime import timedelta
from time import time


def convertCol(df, col):
    df_ = df.copy()
    affected = df_.loc[df_[col] == 2400].index.tolist()
    df_[col] = df_[col].apply(lambda time: "{:02d}:{:02d}".format(
        int(time//100), int(time % 100)))
    df_[col] = df_["FL_DATE"] + " " + df_[col]
    if len(affected) > 0:
        for case in affected:
            date = pd.to_datetime(
                df_.at[case, "FL_DATE"], format="%Y-%m-%d") + timedelta(days=1)
            df_.at[case, col] = date.strftime("%Y-%m-%d %H:%M")
    return pd.to_datetime(df_[col], format="%Y-%m-%d %H:%M")


def importMultipleCSVFiles(directory):
    dfs = []

    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            print("Reading {}".format(filename))
            file = os.path.join(directory, filename)
            dfs.append(pd.read_csv(file))

    df = pd.concat(dfs)
    df = df.reset_index(drop=True)

    return df


def loadPicklesTail(directory, max_files=1000):
    tails = []
    counter = 0

    for filename in os.listdir(directory):
        if filename.startswith("tails") and filename.endswith(".p"):
            print("Reading {}".format(filename))
            file = os.path.join(directory, filename)
            tails_tmp = []
            with open(file, "rb") as fp:
                tails_tmp = pickle.load(fp)
            tails = tails + tails_tmp
            counter += 1
            if counter >= max_files:
                break

    return tails


def loadPicklesData(directory, max_files=1000):
    tails = []
    counter = 0

    for filename in os.listdir(directory):
        if filename.startswith("data") and filename.endswith(".p"):
            print("Reading {}".format(filename))
            file = os.path.join(directory, filename)
            tails_tmp = []
            with open(file, "rb") as fp:
                tails_tmp = pickle.load(fp)
            tails = tails + tails_tmp
            counter += 1
            if counter >= max_files:
                break

    return tails


with open("pickle/chk1.p", "rb") as fp:
    df = pickle.load(fp)

df = df.reset_index(drop=True)

max_turnaround = 3  # in hours

## Tail, airport, atd, ata, turn
turnaround = []

tails = list(set(df["TAIL_NUM"]))
counter = 0

print("{} Tailnumbers".format(len(tails)))
print("{} Flights".format(len(df)))


def pickle_me(topickle):
    tails = [e[0] for e in topickle]
    data = [e[1] for e in topickle]
    timestamp = str(int(time()))

    with open("pickle/tails" + timestamp + ".p", "wb") as fp:
        pickle.dump(tails, fp)
    with open("pickle/data" + timestamp + ".p", "wb") as fp:
        pickle.dump(data, fp)

    del topickle[:]
    del topickle
    del tails
    del data[:]
    del data


def workerTails(tail):
    df_tail = df.loc[df["TAIL_NUM"] == tail]
    df_tail = df_tail.sort_values(by=['STD'])
    df_tail = df_tail.reset_index(drop=True)

    turns = []

    for index in range(0, len(df_tail)-1):
        row1 = df_tail.iloc[index]
        row2 = df_tail.iloc[index + 1]
        aptto = row1["DEST"]
        aptfrom = row2["ORIGIN"]
        if aptto == aptfrom:
            actual_turn = (row2["ATD"] - row1["ATA"]).total_seconds() / 60.0
            scheduled_turn = (row2["STD"] - row1["STA"]).total_seconds() / 60.0
            turn_raw = scheduled_turn - row1["TAXI_IN"] - row2["TAXI_OUT"]
            turn_diff = scheduled_turn - actual_turn

            if abs(scheduled_turn) < max_turnaround * 60:
                turnset = [row1["TAIL_NUM"], aptto,
                           row1["FLIGHT"], row2["FLIGHT"], row1["SFT"], row2["SFT"],
                           row2["ATD"], row1["ATA"],
                           row2["STD"], row1["STA"], row1["TAXI_IN"], row2["TAXI_OUT"],
                           actual_turn, scheduled_turn, turn_raw, turn_diff,
                           row1["ARR_DELAY"], row2["DEP_DELAY"],
                           row1["ATA"].weekday(
                ), row1["ATA"].day, row1["ATA"].month,
                    row1["ATA"].hour, row1["ATA"].hour * 60 + row1["ATA"].minute]
                turns.append(turnset)

    return tail, turns


p = Pool()
counter = 0

prelim_turn = []

tasks = tails[:10]

# Process all turnaround times and pickle every 500
for _ in tqdm(p.imap_unordered(workerTails, tasks), total=len(tasks)):
    prelim_turn.append(_)
    counter += 1
    if counter % 500 == 0:
        print("{} starting pickled".format(counter))
        p = Process(target=pickle_me, args=[prelim_turn[:]])
        p.start()
        del prelim_turn[:]
        del prelim_turn
        prelim_turn = []
        print("{} pickled".format(counter))

print(len(prelim_turn))

pickle_me(prelim_turn[:])

result = loadPicklesData("pickle/")

result_df = []

for tail in result:
    result_df += tail

df_ta = pd.DataFrame(result_df, columns=["Tail", "Airport", 
                                          "Flight1", "Flight2", "Dur1", "Dur2",
                                          "ATD", "ATA", "STD", "STA", "TI", "TO",
                                          "Actual_Turn", "Schedule_Turn", "Raw_Turn", "Turn_Diff", 
                                          "Arr_Delay", "Dep_Delay", 
                                          "Day_of_Week", "Day_of_Month", "Month",
                                          "ATA_Hour", "ATA_Time_Of_Day"])

df_ta.to_csv("source/turnarounds_2018.csv")