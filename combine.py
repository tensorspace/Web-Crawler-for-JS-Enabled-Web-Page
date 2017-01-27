#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date
from workdays import workday
import io

def main():
    holiday = [date(year=2016,month=4,day=4), date(year=2016,month=5,day=2), date(year=2016,month=6,day=9), date(year=2016,month=6,day=10), date(year=2016,month=9,day=15), date(year=2016,month=9,day=16)]
    output_file = io.open('combined_data.csv', 'w+', encoding='utf8')
    category_name = ['轻量级组', '重量级组', '基金组', '程序化组', '金融期货', '有色金属', '贵金属', '农产品', '能源化工', '净利润', '期权']
    for i in range(1, 2):
        dt = workday(date(2016, 3, 31), i, holiday)
        csv_file = io.open(str(dt)+'.csv', 'rU', encoding='utf8')
        lines = csv_file.readlines()
        rank = 0
        category = -1
        for line in lines:
            comma = [i for i in range(len(line)) if line[i] == ',']
            rank = int(line[0:comma[0]])
            if rank == 1:
                category +=1
            if category in range(0, 4):
                output_line = line[comma[0]:comma[1]+1]+ ',' + category_name[category]+','+str(dt) + ',' + line[:comma[0]]+line[comma[1]:]
                output_file.write(output_line)
            elif category in range(4, 10):
                output_line = line[comma[0]:comma[2]+1]+category_name[category]+','+str(dt)+','+line[:comma[0]]+line[comma[2]:comma[5]]+','+line[comma[5]:comma[6]]+','+line[comma[6]:comma[8]]+',,'+line[comma[8]:]
                output_file.write(output_line)
            else:
                output_line = line[comma[0]:comma[1]+1]+','+category_name[category]+','+str(dt)+','+line[:comma[0]]+',,'+line[comma[1]:comma[2]]+','+line[comma[2]:comma[3]]+','+line[comma[3]:comma[5]]+',,'+line[comma[5]:]
                output_file.write(output_line)

if __name__ == '__main__':
    main()
