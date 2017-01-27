#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io, re, json, datetime

def main():
    csv_file = io.open('combined_data.csv', 'rU', encoding='utf8')
    id_output = io.open('id.json', 'w+')
    ambiguity_output = io.open('ambiguity.json', 'w+')
    id_pool = {} # dictionary to store user names
    ambiguity = {}
    lines = csv_file.readlines()
    line_num = 0
    for line in lines:
        line_num += 1
        comma = [i for i in range(len(line)) if line[i] == ',']
        category = line[comma[2] + 1:comma[3]] # extract the attribute of category from csv file
        if category[0] in ['净', '期']:
            name = line[comma[0] + 1:comma[1]] # extract the attribute of user name from csv file
            if re.search(r'\*', name) == None:
                if id_pool.get(name) == None:
                    id_pool[name] = len(id_pool) + 1
            else:
                dt = line[comma[3] + 1:comma[4]] # extract the attribute of date from csv file
                daily_return = line[comma[11] + 1:comma[12]]
                cum_return = line[comma[12] + 1:comma[13]]
                if ambiguity.get(name) == None:
                    ambiguity[name] = {}
                if ambiguity[name].get(dt) == None:
                    ambiguity[name][dt] = []
                ambiguity[name][dt].append((daily_return, cum_return))

    ambiguity_json = {}
    txt_file = open('register.csv', 'w+')
    for name in ambiguity:
        last_dt = []
        max_freq = 0
        count = 0
        for dt in ambiguity[name]:
            if len(ambiguity[name][dt]) >= max_freq:
                max_freq = len(ambiguity[name][dt])
            if len(ambiguity[name][dt]) > 1:
                count += 1
                last_dt.append(dt)
        last_dt = [datetime.datetime.strptime(dt, '%Y-%m-%d').date() for dt in last_dt]
        if len(last_dt) == 0:
            txt_file.write(name + ',' + str(count) + ',' + '\n')
        else:
            last_dt.sort(reverse=True)
            # last_dt = [str(dt) for dt in last_dt]
            # last_dt =  ''.join(last_dt)
            txt_file.write(name+','+ str(count) + ',' + str(max(last_dt)) + '\n')
        if max_freq == 1 or max(last_dt) != datetime.date(year=2016,month=9,day=30):
            id_pool[name] = len(id_pool) + 1
        else:
            ambiguity_json[name] = ambiguity[name]

    json.dump(ambiguity_json, ambiguity_output, ensure_ascii=False)
    json.dump(id_pool, id_output, ensure_ascii=False)

if __name__ == '__main__':
    main()
