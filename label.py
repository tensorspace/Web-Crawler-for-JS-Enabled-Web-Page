import io, re, json, datetime

def main():
    csv_file = io.open('spbs.csv', 'rU', encoding='utf8')
    id_file = io.open('id.json', 'rU')
    disambibuity_file = io.open('disambiguity.json', 'rU')
    csv_output = io.open('spds.csv', 'w+', encoding='utf8')
    csv_output.write('客户代码,客户昵称,组别,排行榜,日期,排名,当日权益,风险度(%),净利润,净利润得分,回撤率(%),回撤率得分,日净值,累计净值,净值得分,综合得分,参考收益率(%),指定交易商,操作指导,账户评估\n')
    id_pool = json.load(id_file) # dictionary to store user names
    id_pool['荣 业'] = len(id_pool) + 1
    disambiguity = json.load(disambibuity_file)
    offset = len(id_pool)
    lines = csv_file.readlines()
    line_num = 0
    error_file = open('error.csv', 'w+')
    for line in lines:
        line_num += 1
        comma = [i for i in range(len(line)) if line[i] == ',']
        name = line[comma[0] + 1:comma[1]] # extract the attribute of user name from csv file
        dt = line[comma[3] + 1:comma[4]]  # extract the attribute of date from csv file
        daily_return = line[comma[11] + 1:comma[12]]
        cum_return = line[comma[12] + 1:comma[13]]
        if id_pool.get(name) != None:
            csv_output.write(str(id_pool[name]) + line)
        else:
            daily_return = str(int(float(daily_return) * 1000))
            cum_return = str(int(float(cum_return) * 1000))
            if disambiguity.get(name + daily_return + cum_return + dt) != None:
                csv_output.write(str(offset + disambiguity[name + daily_return + cum_return + dt]) + line)
            else:
                csv_output.write('999999'+ line)
                error_file.write(line)

if __name__ == '__main__':
    main()
