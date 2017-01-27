#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io, datetime, json

def main():
    ambiguity_json = io.open('ambiguity.json', 'rU')
    ambiguity = json.load(ambiguity_json)
    disambiguity = {}
    for name in ambiguity:
        print(name)
        dates = [datetime.datetime.strptime(dt, '%Y-%m-%d').date() for dt in ambiguity[name].keys()]
        dates.sort()
        disambiguity[name] = []
        for dt in dates:
            print(dt)
            temp = ambiguity[name][str(dt)]
            for candidate in disambiguity[name]:
                if len(temp) > 0:
                    difference = [abs(float(candidate[-1][1]) * float(x[0]) - float(x[1])) for x in temp]
                    match = temp[difference.index(min(difference))]
                    candidate.append((match[0], match[1], str(dt)))
                    del match
                else:
                    break
            while len(temp) > 0:
                disambiguity[name].append([])
                disambiguity[name][-1].append((temp[0][0], temp[0][1], str(dt)))
                del temp[0]

    index = 0
    index_json = {}
    for name in disambiguity:
        for candidate in disambiguity[name]:
            index += 1
            for item in candidate:
                daily_return = str(int(float(item[0]) * 1000))
                cum_return = str(int(float(item[1]) * 1000))
                index_json[name + daily_return + cum_return + item[2]] = index

    disambiguity_output = io.open('disambiguity.json', 'w+')
    json.dump(index_json, disambiguity_output, ensure_ascii=False)

if __name__ == '__main__':
    main()
